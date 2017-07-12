package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import cats.Id
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.{ErrorsOr, TemplatesRepo}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.http.HttpClient
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.kafka.consumers.{
  CancellationRequestConsumer,
  LegacyCancellationRequestConsumer,
  LegacyTriggeredConsumer,
  TriggeredConsumer
}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.{ChannelSelector, ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.retry.Retry
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata
import com.ovoenergy.comms.akka.streams.Factory.{KafkaConfig, consumerSettings}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings

object Main extends App with LoggingWithMDC {

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  val config: Config = ConfigFactory.load()

  implicit val actorSystem  = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val region = Regions.fromName(config.getString("aws.region"))
  val isRunningInLocalDocker = sys.env.get("ENV").contains("LOCAL") && sys.env
      .get("RUNNING_IN_DOCKER")
      .contains("true")

  val schedulingPersistence = new DynamoPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = DynamoPersistence.Context(
      db = AwsProvider.dynamoClient(isRunningInLocalDocker, region),
      tableName = config.getString("scheduling.persistence.table")
    )
  )

  val kafkaSettings = new KafkaSettings(config)

  val templatesContext = AwsProvider.templatesContext(isRunningInLocalDocker, region)

  val retrieveTemplate: (CommManifest) => ErrorsOr[CommTemplate[Id]] = TemplatesRepo.getTemplate(templatesContext, _)
  val determineChannel                                               = new ChannelSelectorWithTemplate(retrieveTemplate)

  val schemaRegistrySettings = kafkaSettings.schemaRegistryClientSettings
  val aivenSslConfig         = kafkaSettings.sslConfig

  val legacyKafkaHosts = config.getString("kafka.hosts")
  val aivenKafkaHosts  = config.getString("kafka.aiven.hosts")

  val kafkaProducerRetryConfig = Retry.RetryConfig(
    attempts = config.getInt("kafka.producer.retry.attempts"),
    backoff = Retry.Backoff.exponential(
      config.getDuration("kafka.producer.retry.initialInterval").toFiniteDuration,
      config.getDouble("kafka.producer.retry.exponent")
    )
  )

  val orchestrateEmail = new IssueOrchestratedEmail(
    Producer[OrchestratedEmailV3](
      hosts = aivenKafkaHosts,
      topic = config.getString("kafka.topics.orchestrated.email.v3"),
      retryConfig = kafkaProducerRetryConfig,
      schemaRegistryClientSettings = schemaRegistrySettings,
      sslConfig = aivenSslConfig
    )
  )

  val orchestrateSMS = new IssueOrchestratedSMS(
    Producer[OrchestratedSMSV2](
      hosts = aivenKafkaHosts,
      topic = config.getString("kafka.topics.orchestrated.sms.v2"),
      retryConfig = kafkaProducerRetryConfig,
      schemaRegistryClientSettings = schemaRegistrySettings,
      sslConfig = aivenSslConfig
    )
  )

  val profileCustomer = {
    val retryConfig = Retry.RetryConfig(
      attempts = config.getInt("profile.http.retry.attempts"),
      backoff = Retry.Backoff.exponential(
        config.getDuration("profile.http.retry.initialInterval").toFiniteDuration,
        config.getDouble("profile.http.retry.exponent")
      )
    )
    CustomerProfiler(
      httpClient = HttpClient.apply,
      profileApiKey = config.getString("profile.service.apiKey"),
      profileHost = config.getString("profile.service.host"),
      retryConfig = retryConfig
    ) _
  }

  val orchestrateComm: (TriggeredV3, InternalMetadata) => Either[ErrorDetails, Future[RecordMetadata]] = Orchestrator(
    profileCustomer = profileCustomer,
    channelSelector = determineChannel,
    issueOrchestratedEmail = orchestrateEmail,
    issueOrchestratedSMS = orchestrateSMS,
    validateProfile = ProfileValidation.apply
  )

  val sendFailedTriggerEvent: FailedV2 => Future[RecordMetadata] = {
    Producer[FailedV2](
      hosts = aivenKafkaHosts,
      topic = config.getString("kafka.topics.failed.v2"),
      retryConfig = kafkaProducerRetryConfig,
      schemaRegistryClientSettings = schemaRegistrySettings,
      sslConfig = aivenSslConfig
    )
  }

  val sendCancelledEvent: (CancelledV2) => Future[RecordMetadata] = Producer[CancelledV2](
    hosts = aivenKafkaHosts,
    topic = config.getString("kafka.topics.scheduling.cancelled.v2"),
    retryConfig = kafkaProducerRetryConfig,
    schemaRegistryClientSettings = schemaRegistrySettings,
    sslConfig = aivenSslConfig
  )

  val sendFailedCancellationEvent: (FailedCancellationV2) => Future[RecordMetadata] =
    Producer[FailedCancellationV2](
      hosts = aivenKafkaHosts,
      topic = config.getString("kafka.topics.scheduling.failedCancellation.v2"),
      retryConfig = kafkaProducerRetryConfig,
      schemaRegistryClientSettings = schemaRegistrySettings,
      sslConfig = aivenSslConfig
    )

  val sendOrchestrationStartedEvent: OrchestrationStartedV2 => Future[RecordMetadata] =
    Producer[OrchestrationStartedV2](
      hosts = aivenKafkaHosts,
      topic = config.getString("kafka.topics.orchestration.started.v2"),
      retryConfig = kafkaProducerRetryConfig,
      schemaRegistryClientSettings = schemaRegistrySettings,
      sslConfig = aivenSslConfig
    )

  val executeScheduledTask = TaskExecutor.execute(schedulingPersistence,
                                                  orchestrateComm,
                                                  sendOrchestrationStartedEvent,
                                                  () => UUID.randomUUID.toString,
                                                  sendFailedTriggerEvent) _
  val addSchedule    = QuartzScheduling.addSchedule(executeScheduledTask) _
  val scheduleTask   = Scheduler.scheduleComm(schedulingPersistence.storeSchedule, addSchedule) _
  val removeSchedule = QuartzScheduling.removeSchedule _
  val descheduleComm = Scheduler.descheduleComm(schedulingPersistence.cancelSchedules, removeSchedule) _

  val schedulingGraph: RunnableGraph[Control] = {
    val kafkaConf = kafkaSettings.getLegacyKafkaConfig("kafka.topics.triggered.v3")
    TriggeredConsumer(
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedTriggerEvent,
      config = kafkaConf,
      generateTraceToken = () => UUID.randomUUID().toString,
      consumerSettings = kafkaSettings.legacyConsumerSettings[TriggeredV3](kafkaConf)
    )
  }

  val legacySchedulingGraph: RunnableGraph[Control] = LegacyTriggeredConsumer(
    scheduleTask = scheduleTask,
    sendFailedEvent = sendFailedTriggerEvent,
    config = kafkaSettings.getLegacyKafkaConfig("kafka.topics.triggered.v2"),
    generateTraceToken = () => UUID.randomUUID().toString
  )

  val aivenSchedulingGraph: RunnableGraph[Control] = {
    val kafkaConf: KafkaConfig = kafkaSettings.getAivenKafkaConfig("kafka.topics.triggered.v3")
    TriggeredConsumer(
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedTriggerEvent,
      config = kafkaConf,
      generateTraceToken = () => UUID.randomUUID().toString,
      consumerSettings = consumerSettings[TriggeredV3](schemaRegistrySettings, kafkaConfig = kafkaConf)
    )
  }

  val aivenCancellationRequestGraph = {
    val kafkaConf = kafkaSettings.getAivenKafkaConfig("kafka.topics.scheduling.cancellationRequest.v2")
    CancellationRequestConsumer(
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      generateTraceToken = () => UUID.randomUUID().toString,
      descheduleComm = descheduleComm,
      config = kafkaConf,
      consumerSettings = consumerSettings[CancellationRequestedV2](schemaRegistrySettings, kafkaConfig = kafkaConf)
    )
  }

  val cancellationRequestGraph: RunnableGraph[Control] = {
    val kafkaConf = kafkaSettings.getLegacyKafkaConfig("kafka.topics.scheduling.cancellationRequest.v2")
    CancellationRequestConsumer(
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      generateTraceToken = () => UUID.randomUUID().toString,
      descheduleComm = descheduleComm,
      config = kafkaConf,
      consumerSettings = kafkaSettings.legacyConsumerSettings[CancellationRequestedV2](kafkaConf)
    )
  }

  val legacyCancellationRequestGraph = LegacyCancellationRequestConsumer(
    sendFailedCancellationEvent = sendFailedCancellationEvent,
    sendSuccessfulCancellationEvent = sendCancelledEvent,
    generateTraceToken = () => UUID.randomUUID().toString,
    descheduleComm = descheduleComm,
    config = kafkaSettings.getLegacyKafkaConfig("kafka.topics.scheduling.cancellationRequest.v1")
  )

  QuartzScheduling.init()

  /*
  For pending schedules, we only need to load them once at startup.
  Actually a few minutes after startup, as we need to wait until this instance
  is consuming Kafka events. If we load the pending schedules while a previous
  instance is still processing events, we might miss some.
   */
  actorSystem.scheduler.scheduleOnce(config.getDuration("scheduling.loadPending.delay").toFiniteDuration) {
    val scheduledCount = Restore.pickUpPendingSchedules(schedulingPersistence, addSchedule)
    log.info(s"Loaded $scheduledCount pending schedules")
  }

  /*
  For expired schedules, we poll every few minutes.
  They should be very rare. Expiry only happens when an instance starts orchestrating
  and then dies half way through.
   */
  val pollForExpiredInterval = config.getDuration("scheduling.pollForExpired.interval")
  log.info(s"Polling for expired schedules with interval: $pollForExpiredInterval")
  actorSystem.scheduler.schedule(1.second, pollForExpiredInterval.toFiniteDuration) {
    try {
      val scheduledCount = Restore.pickUpExpiredSchedules(schedulingPersistence, addSchedule)
      log.info(s"Recovered $scheduledCount expired schedules")
    } catch {
      case e: AmazonDynamoDBException =>
        log.warn("Failed to poll for expired schedules", e)
    }
  }

  def runGraph(graphName: String, graph: RunnableGraph[Consumer.Control]) = {
    log.debug(s"Starting graph: $graph")
    val control = graph.run()
    control.isShutdown.foreach { _ =>
      log.error(s"ARGH! The Kafka $graphName event source has shut down. Killing the JVM and nuking from orbit.")
      System.exit(1)
    }
  }

  runGraph("scheduling", schedulingGraph)
  runGraph("aiven scheduling", aivenSchedulingGraph)
  runGraph("legacy scheduling", legacySchedulingGraph)
  runGraph("cancellation", cancellationRequestGraph)
  runGraph("aiven cancellation", aivenCancellationRequestGraph)
  runGraph("legacy cancellation", legacyCancellationRequestGraph)
  log.info("Orchestration started")
}
