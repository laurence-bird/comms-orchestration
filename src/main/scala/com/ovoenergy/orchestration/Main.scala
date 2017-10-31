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
import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.{ErrorsOr, TemplatesRepo}
import com.ovoenergy.orchestration.ErrorHandling._
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
import com.ovoenergy.orchestration.processes.{ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.comms.serialisation.Retry
import com.ovoenergy.comms.serialisation.Retry.RetryConfig

object Main extends App with LoggingWithMDC {
  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit val config: Config = ConfigFactory.load()

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

  val templatesContext = AwsProvider.templatesContext(isRunningInLocalDocker, region)

  val retrieveTemplate: (CommManifest) => ErrorsOr[CommTemplate[Id]] = TemplatesRepo.getTemplate(templatesContext, _)
  val determineChannel                                               = new ChannelSelectorWithTemplate(retrieveTemplate)

  val sendFailedTriggerEvent = {
    val topic = Kafka.aiven.failed.v2
    exitAppOnFailure(topic.retryPublisher, topic.name)
  }
  val sendCancelledEvent = {
    val topic = Kafka.aiven.cancelled.v2
    exitAppOnFailure(topic.retryPublisher, topic.name)
  }
  val sendFailedCancellationEvent   = retryPublisherFor(Kafka.aiven.failedCancellation.v2)
  val sendOrchestrationStartedEvent = retryPublisherFor(Kafka.aiven.orchestrationStarted.v2)
  val sendOrchestratedEmailEvent    = retryPublisherFor(Kafka.aiven.orchestratedEmail.v3)
  val sendOrchestratedSMSEvent      = retryPublisherFor(Kafka.aiven.orchestratedSMS.v2)
  val sendOrchestratedPrintEvent    = retryPublisherFor(Kafka.aiven.orchestratedPrint.v1)

  val orchestrateEmail = new IssueOrchestratedEmail(sendOrchestratedEmailEvent)
  val orchestrateSMS   = new IssueOrchestratedSMS(sendOrchestratedSMSEvent)
  val orchestratePrint = new IssueOrchestratedPrint(sendOrchestratedPrintEvent)

  val profileCustomer = {
    val retryConfig = RetryConfig(
      attempts = config.getInt("profile.http.retry.attempts"),
      initialInterval = config.getDuration("profile.http.retry.initialInterval").toFiniteDuration,
      exponent = config.getDouble("profile.http.retry.exponent")
    )

    CustomerProfiler(
      httpClient = HttpClient.apply,
      profileApiKey = config.getString("profile.service.apiKey"),
      profileHost = config.getString("profile.service.host"),
      retryConfig = retryConfig
    ) _
  }

  val orchestrateComm: (TriggeredV3, InternalMetadata) => Either[ErrorDetails, Future[RecordMetadata]] = Orchestrator(
    channelSelector = determineChannel,
    getValidatedCustomerProfile = ProfileValidation.getValidatedCustomerProfile(profileCustomer),
    getValidatedContactProfile = ProfileValidation.getValidatedContactProfile,
    issueOrchestratedEmail = orchestrateEmail,
    issueOrchestratedSMS = orchestrateSMS,
    issueOrchestratedPrint = orchestratePrint
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

  val legacyV3: RunnableGraph[Control] = {
    TriggeredConsumer(
      topic = Kafka.legacy.triggered.v3,
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedTriggerEvent,
      generateTraceToken = () => UUID.randomUUID().toString
    )
  }

  val legacyV2: RunnableGraph[Control] = LegacyTriggeredConsumer(
    topic = Kafka.legacy.triggered.v2,
    scheduleTask = scheduleTask,
    sendFailedEvent = sendFailedTriggerEvent,
    generateTraceToken = () => UUID.randomUUID().toString
  )

  val aivenV3: RunnableGraph[Control] = {
    TriggeredConsumer(
      topic = Kafka.aiven.triggered.v3,
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedTriggerEvent,
      generateTraceToken = () => UUID.randomUUID().toString
    )
  }

  val aivenCancellationRequestGraph = {
    CancellationRequestConsumer(
      topic = Kafka.aiven.cancellationRequested.v2,
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      generateTraceToken = () => UUID.randomUUID().toString,
      descheduleComm = descheduleComm
    )
  }

  val cancellationRequestGraph: RunnableGraph[Control] = {
    CancellationRequestConsumer(
      topic = Kafka.legacy.cancellationRequested.v2,
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      generateTraceToken = () => UUID.randomUUID().toString,
      descheduleComm = descheduleComm
    )
  }

  val legacyCancellationRequestGraph = LegacyCancellationRequestConsumer(
    topic = Kafka.legacy.cancellationRequested.v1,
    sendFailedCancellationEvent = sendFailedCancellationEvent,
    sendSuccessfulCancellationEvent = sendCancelledEvent,
    generateTraceToken = () => UUID.randomUUID().toString,
    descheduleComm = descheduleComm
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

  runGraph("scheduling", legacyV3)
  runGraph("aiven scheduling", aivenV3)
  runGraph("legacy scheduling", legacyV2)
  runGraph("cancellation", cancellationRequestGraph)
  runGraph("aiven cancellation", aivenCancellationRequestGraph)
  runGraph("legacy cancellation", legacyCancellationRequestGraph)
  log.info("Orchestration started")
}
