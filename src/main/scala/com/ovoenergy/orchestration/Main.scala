package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.Id
import cats.effect.{Async, IO}
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ProvisionedThroughputExceededException}
import com.ovoenergy.comms.helpers.{Kafka, Topic}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model.print.OrchestratedPrint
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.{ErrorsOr, TemplatesRepo}
import com.ovoenergy.orchestration.ErrorHandling._
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.http.HttpClient
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.kafka.consumers.{CancellationRequestConsumer, KafkaConsumer, TriggeredConsumer}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.{ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.scheduling.dynamo.{AsyncPersistence, DynamoPersistence}
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata
import fs2._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.orchestration.http.Retry.RetryConfig
import com.ovoenergy.orchestration.kafka.consumers.KafkaConsumer.Record
import com.ovoenergy.orchestration.util.Retry
import fs2.StreamApp

import scala.util.control.NonFatal

object Main extends StreamApp[IO] with LoggingWithMDC with ExecutionContexts {
  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit val ec = globalExecutionContext

  implicit val config: Config = ConfigFactory.load()
  implicit val actorSystem    = ActorSystem()
  implicit val materializer   = ActorMaterializer()

  lazy val triggeredTopic: Topic[TriggeredV3]                         = Kafka.aiven.triggered.v3
  lazy val cancellationRequestedTopic: Topic[CancellationRequestedV2] = Kafka.aiven.cancellationRequested.v2

  val region = Regions.fromName(config.getString("aws.region"))
  val isRunningInLocalDocker = sys.env.get("ENV").contains("LOCAL") && sys.env
      .get("RUNNING_IN_DOCKER")
      .contains("true")

  val dynamoContext = DynamoPersistence.Context(
    dbClients = AwsProvider.dynamoClients(isRunningInLocalDocker, region),
    tableName = config.getString("scheduling.persistence.table")
  )

  // Until we rethink our scheduling architecture, we need to use the bloking synchronous
  // db client for the task executor, due to multithreading hell
  val schedulingPersistence = new DynamoPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = dynamoContext
  )

  private val retryMaxRetries: Int       = 5
  private val retryDelay: FiniteDuration = 5.seconds

  def retry = Retry(retryMaxRetries, retryDelay)

  val consumerSchedulingPersistence = new AsyncPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = dynamoContext
  )

  val templatesContext = AwsProvider.templatesContext(isRunningInLocalDocker, region)

  val retrieveTemplate: (CommManifest) => ErrorsOr[CommTemplate[Id]] = TemplatesRepo.getTemplate(templatesContext, _)
  val determineChannel                                               = new ChannelSelectorWithTemplate(retrieveTemplate)

  // TODO: Confirm the partition key with the guys
  val sendFailedTriggerEvent = publisherFor[FailedV2](Kafka.aiven.failed.v2, _.metadata.eventId)
  val sendCancelledEvent     = publisherFor[CancelledV2](Kafka.aiven.cancelled.v2, _.metadata.eventId)
  val sendFailedCancellationEvent =
    publisherFor[FailedCancellationV2](Kafka.aiven.failedCancellation.v2, _.metadata.eventId)
  val sendOrchestrationStartedEvent =
    publisherFor[OrchestrationStartedV2](Kafka.aiven.orchestrationStarted.v2, _.metadata.eventId)
  val sendOrchestratedEmailEvent =
    publisherFor[OrchestratedEmailV3](Kafka.aiven.orchestratedEmail.v3, _.metadata.eventId)
  val sendOrchestratedSMSEvent =
    publisherFor[OrchestratedSMSV2](Kafka.aiven.orchestratedSMS.v2, _.metadata.eventId)
  val sendOrchestratedPrintEvent =
    publisherFor[OrchestratedPrint](Kafka.aiven.orchestratedPrint.v1, _.metadata.eventId)

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

  val orchestrateComm: (TriggeredV3, InternalMetadata) => Either[ErrorDetails, IO[RecordMetadata]] = Orchestrator[IO](
    channelSelector = determineChannel,
    getValidatedCustomerProfile = ProfileValidation.getValidatedCustomerProfile(profileCustomer),
    getValidatedContactProfile = ProfileValidation.validateContactProfile,
    issueOrchestratedEmail = orchestrateEmail,
    issueOrchestratedSMS = orchestrateSMS,
    issueOrchestratedPrint = orchestratePrint
  )

  val executeScheduledTask = TaskExecutor.execute(schedulingPersistence,
                                                  orchestrateComm,
                                                  sendOrchestrationStartedEvent,
                                                  () => UUID.randomUUID.toString,
                                                  sendFailedTriggerEvent,
                                                  blockingExecutionContext) _
  val addSchedule = QuartzScheduling.addSchedule(executeScheduledTask) _

  def scheduleTask = { t: TriggeredV3 =>
    val res = Scheduler.scheduleComm(consumerSchedulingPersistence.storeSchedule[IO], addSchedule).apply(t)
    retry(
      res,
      _.isInstanceOf[ProvisionedThroughputExceededException]
    ).recoverWith {
      case e: ProvisionedThroughputExceededException =>
        logError("Failed to write Kafka record to DynamoDb, failing the stream", e)
        IO.raiseError(e)

      case e: AmazonDynamoDBException if e.getErrorCode == "ValidationException" =>
        logWarn("Failed to write Kafka record to DynamoDb, skipping the record", e)
        IO.pure(Left(ErrorDetails("Failed to write schedule to dynamoDB", OrchestrationError)))

      case NonFatal(e) =>
        logError("Failed to write Kafka record to DynamoDb, failing the stream", e)
        IO.raiseError(e)
    }
  }

  val removeSchedule = QuartzScheduling.removeSchedule _
  val descheduleComm = Scheduler.descheduleComm(schedulingPersistence.cancelSchedules, removeSchedule) _

  val triggeredConsumer = {
    TriggeredConsumer[IO](
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedTriggerEvent.andThen(_.map(_ => ())),
      generateTraceToken = () => UUID.randomUUID().toString
    )
  }

  val cancellationRequestConsumer = {
    CancellationRequestConsumer(
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      descheduleComm = descheduleComm,
      generateTraceToken = () => UUID.randomUUID().toString
    )
  }

  QuartzScheduling.init()

  /*
  For pending schedules, we only need to load them once at startup.
  Actually a few minutes after startup, as we need to wait until this instance
  is consuming Kafka events. If we load the pending schedules while a previous
  instance is still processing events, we might miss some.
   */
  // TODO: Change to use FS2
  actorSystem.scheduler.scheduleOnce(config.getDuration("scheduling.loadPending.delay").toFiniteDuration) {
    val scheduledCount = Restore.pickUpPendingSchedules(schedulingPersistence, addSchedule)
    log.info(s"Loaded $scheduledCount pending schedules")
  }(akkaExecutionContext)

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
  }(akkaExecutionContext)

  import fs2._

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, StreamApp.ExitCode] = {
    // TODO: Improve error handling here
    def recordProcessor[T, Result](consume: T => IO[Result]): Record[T] => IO[Unit] = { record: Record[T] =>
      record.value() match {
        case Some(r) => consume(r).map(_ => ())
        case None =>
          logError(
            s"Skipping event: $record, failed to parse",
            Map(
              "offset"    -> record.offset().toString,
              "partition" -> record.partition().toString,
              "topic"     -> record.topic()
            )
          )
          IO.pure(())
      }
    }

    val triggeredStream = KafkaConsumer[IO](topic = triggeredTopic)(f = recordProcessor(triggeredConsumer))
    val cancellationRequestStream =
      KafkaConsumer[IO](topic = cancellationRequestedTopic)(f = recordProcessor(cancellationRequestConsumer))

    val s = Stream.eval(IO(log.info("Orchestration started"))) >>
        triggeredStream.mergeHaltBoth(cancellationRequestStream)

    s.drain.covaryOutput[StreamApp.ExitCode] ++ Stream.emit(StreamApp.ExitCode.Error)
  }
}
