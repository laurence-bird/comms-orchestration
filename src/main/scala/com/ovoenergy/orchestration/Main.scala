package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.Id
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.{ErrorsOr, TemplatesContext, TemplatesRepo}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.http.HttpClient
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.domain.HasIds._
import com.ovoenergy.orchestration.kafka.consumers.{CancellationRequestConsumer, TriggeredConsumer}
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.{ChannelSelector, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.retry.Retry
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

object Main extends App with LoggingWithMDC {

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  val config = ConfigFactory.load()

  implicit val actorSystem  = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val region             = Regions.fromName(config.getString("aws.region"))
  val isRunningInCompose = sys.env.get("DOCKER_COMPOSE").contains("true")

  val schedulingPersistence = new DynamoPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = DynamoPersistence.Context(
      db = AwsProvider.dynamoClient(isRunningInCompose, region),
      tableName = config.getString("scheduling.persistence.table")
    )
  )

  val templatesContext = AwsProvider.templatesContext(isRunningInCompose, region)

  val retrieveTemplate: (CommManifest) => ErrorsOr[CommTemplate[Id]] = TemplatesRepo.getTemplate(templatesContext, _)
  val determineChannel                                               = ChannelSelector.determineChannel(retrieveTemplate) _

  val kafkaHosts = config.getString("kafka.hosts")
  val kafkaProducerRetryConfig = Retry.RetryConfig(
    attempts = config.getInt("kafka.producer.retry.attempts"),
    backoff = Retry.Backoff.exponential(
      config.getDuration("kafka.producer.retry.initialInterval").toFiniteDuration,
      config.getDouble("kafka.producer.retry.exponent")
    )
  )

  val orchestrateEmail: (CustomerDeliveryDetails, TriggeredV2, InternalMetadata) => Future[RecordMetadata] =
    OrchestratedEmailEvent.send(
      Producer(
        hosts = kafkaHosts,
        topic = config.getString("kafka.topics.orchestrated.email.v2"),
        serialiser = Serialisation.orchestratedEmailV2Serializer,
        retryConfig = kafkaProducerRetryConfig
      )
    )

  val orchestrateSMS = OrchestratedSMSEvent.send(
    Producer(
      hosts = kafkaHosts,
      topic = config.getString("kafka.topics.orchestrated.sms"),
      serialiser = Serialisation.orchestratedSMSV2Serializer,
      retryConfig = kafkaProducerRetryConfig
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

  val orchestrateComm: (TriggeredV2, InternalMetadata) => Either[ErrorDetails, Future[RecordMetadata]] = Orchestrator(
    profileCustomer = profileCustomer,
    determineChannel = determineChannel,
    sendOrchestratedEmailEvent = orchestrateEmail,
    sendOrchestratedSMSEvent = orchestrateSMS,
    validateProfile = ProfileValidation.apply
  )

  val sendFailedTriggerEvent: Failed => Future[RecordMetadata] = {
    Producer(
      hosts = kafkaHosts,
      topic = config.getString("kafka.topics.failed"),
      serialiser = Serialisation.failedSerializer,
      retryConfig = kafkaProducerRetryConfig
    )
  }

  val sendCancelledEvent: (Cancelled) => Future[RecordMetadata] = Producer(
    hosts = kafkaHosts,
    topic = config.getString("kafka.topics.scheduling.cancelled"),
    serialiser = Serialisation.cancelledSerializer,
    retryConfig = kafkaProducerRetryConfig
  )

  val sendFailedCancellationEvent: (FailedCancellation) => Future[RecordMetadata] =
    Producer(
      hosts = kafkaHosts,
      topic = config.getString("kafka.topics.scheduling.failedCancellation"),
      serialiser = Serialisation.failedCancellationSerializer,
      retryConfig = kafkaProducerRetryConfig
    )

  val sendOrchestrationStartedEvent: OrchestrationStarted => Future[RecordMetadata] =
    Producer(
      hosts = kafkaHosts,
      topic = config.getString("kafka.topics.orchestration.started"),
      serialiser = Serialisation.orchestrationStartedSerializer,
      retryConfig = kafkaProducerRetryConfig
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

  val schedulingGraph = TriggeredConsumer(
    consumerDeserializer = Serialisation.triggeredV2Deserializer,
    scheduleTask = scheduleTask,
    sendFailedEvent = sendFailedTriggerEvent,
    config = KafkaConfig(
      hosts = kafkaHosts,
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered.v2")
    ),
    generateTraceToken = () => UUID.randomUUID().toString
  )

  val cancellationRequestGraph = CancellationRequestConsumer(
    sendFailedCancellationEvent = sendFailedCancellationEvent,
    sendSuccessfulCancellationEvent = sendCancelledEvent,
    generateTraceToken = () => UUID.randomUUID().toString,
    descheduleComm = descheduleComm,
    config = KafkaConfig(
      hosts = kafkaHosts,
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.scheduling.cancellationRequest")
    )
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

  val schedulingControl   = schedulingGraph.run()
  val cancellationControl = cancellationRequestGraph.run()

  cancellationControl.isShutdown.foreach { _ =>
    log.error("ARGH! The Kafka cancellation event source has shut down. Killing the JVM and nuking from orbit.")
    System.exit(1)
  }

  schedulingControl.isShutdown.foreach { _ =>
    log.error("ARGH! The Kafka triggered event source has shut down. Killing the JVM and nuking from orbit.")
    System.exit(1)
  }

  log.info("Orchestration started")
}
