package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.effect.IO
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ProvisionedThroughputExceededException}
import com.ovoenergy.orchestration.ErrorHandling.publisherFor
import com.ovoenergy.comms.helpers.{Kafka, Topic}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.comms.model.print.OrchestratedPrintV2
import com.ovoenergy.comms.model.sms.OrchestratedSMSV3
import com.ovoenergy.comms.templates.{ErrorsOr, TemplateMetadataContext}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.kafka.consumers._
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.ovoenergy.orchestration.processes.{ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.scheduling.dynamo.{AsyncPersistence, DynamoPersistence}
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, Schedule, TaskExecutor}
import com.ovoenergy.orchestration.kafka.consumers.EventConverter._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{RecordMetadata}
import fs2._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.ovoenergy.comms.templates.cache.CachingStrategy
import com.ovoenergy.comms.templates.model.template.metadata.TemplateId
import com.ovoenergy.orchestration.kafka.consumers.KafkaConsumer.Record
import com.ovoenergy.orchestration.templates.RetrieveTemplateDetails
import com.ovoenergy.orchestration.util.Retry
import fs2.StreamApp
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.client.blaze.Http1Client

import scala.util.control.NonFatal

object Main extends StreamApp[IO] with LoggingWithMDC with ExecutionContexts {
  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit val ec = executionContext

  implicit val config: Config = ConfigFactory.load()
  implicit val actorSystem    = ActorSystem()
  implicit val materializer   = ActorMaterializer()

  lazy val triggeredV3Topic: Topic[TriggeredV3]                         = Kafka.aiven.triggered.v3
  lazy val triggeredV4Topic: Topic[TriggeredV4]                         = Kafka.aiven.triggered.v4
  lazy val cancellationRequestedV2Topic: Topic[CancellationRequestedV2] = Kafka.aiven.cancellationRequested.v2
  lazy val cancellationRequestedV3Topic: Topic[CancellationRequestedV3] = Kafka.aiven.cancellationRequested.v3

  val region = Regions.fromName(config.getString("aws.region"))
  val isRunningInLocalDocker = sys.env.get("ENV").contains("LOCAL") && sys.env
      .get("RUNNING_IN_DOCKER")
      .contains("true")

  val templateSummaryTableName = config.getString("template.summary.table")

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

  val dbClients = AwsProvider.dynamoClients(isRunningInLocalDocker, region)

  val consumerSchedulingPersistence = new AsyncPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = dynamoContext
  )

  val templatesContext        = AwsProvider.templatesContext(isRunningInLocalDocker, region)
  val templateMetadataContext = TemplateMetadataContext(dynamoContext.asyncDb, templateSummaryTableName)
  val retrieveTemplateDetails = RetrieveTemplateDetails[IO](
    templatesContext,
    templateMetadataContext,
    CachingStrategy.caffeine[TemplateId, ErrorsOr[CommType]](250))
  val determineChannel = new ChannelSelectorWithTemplate(retrieveTemplateDetails)

  val sendFailedEvent    = publisherFor[FailedV3](Kafka.aiven.failed.v3, _.metadata.eventId)
  val sendCancelledEvent = publisherFor[CancelledV3](Kafka.aiven.cancelled.v3, _.metadata.eventId)
  val sendFailedCancellationEvent =
    publisherFor[FailedCancellationV3](Kafka.aiven.failedCancellation.v3, _.metadata.eventId)
  val sendOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata] =
    publisherFor[OrchestrationStartedV3](Kafka.aiven.orchestrationStarted.v3, _.metadata.eventId)
  val sendOrchestratedEmailEvent =
    publisherFor[OrchestratedEmailV4](Kafka.aiven.orchestratedEmail.v4, _.metadata.eventId)
  val sendOrchestratedSMSEvent =
    publisherFor[OrchestratedSMSV3](Kafka.aiven.orchestratedSMS.v3, _.metadata.eventId)
  val sendOrchestratedPrintEvent =
    publisherFor[OrchestratedPrintV2](Kafka.aiven.orchestratedPrint.v2, _.metadata.eventId)

  val orchestrateEmail = new IssueOrchestratedEmail(sendOrchestratedEmailEvent)
  val orchestrateSMS   = new IssueOrchestratedSMS(sendOrchestratedSMSEvent)
  val orchestratePrint = new IssueOrchestratedPrint(sendOrchestratedPrintEvent)

  private val httpClient: Client[IO] = Http1Client[IO]().unsafeRunSync

  val profileCustomer = CustomerProfiler[IO](
    client = httpClient,
    uri = Uri.unsafeFromString(config.getString("profile.service.host")),
    retry = Retry.backOff(
      config.getInt("profile.http.retry.attempts"),
      config.getDuration("profile.http.retry.initialInterval").toFiniteDuration,
      config.getDouble("profile.http.retry.exponent")
    ),
    apiKey = config.getString("profile.service.apiKey")
  )

  val orchestrateComm: (TriggeredV4, InternalMetadata) => IO[Either[ErrorDetails, RecordMetadata]] = Orchestrator[IO](
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
                                                  sendFailedEvent) _
  val addSchedule = QuartzScheduling.addSchedule(executeScheduledTask) _

  val scheduleTask: TriggeredV4 => IO[Either[ErrorDetails, Boolean]] = { t: TriggeredV4 =>
    val res = Scheduler.scheduleComm(consumerSchedulingPersistence.storeSchedule[IO], addSchedule).apply(t)
    retry(
      res,
      _.isInstanceOf[ProvisionedThroughputExceededException]
    ).recoverWith {
      case e: ProvisionedThroughputExceededException =>
        failWithException("Failed to write Kafka record to DynamoDb, failing the stream")(e)
        IO.raiseError(e)

      case e: AmazonDynamoDBException if e.getErrorCode == "ValidationException" =>
        warnWithException("Failed to write Kafka record to DynamoDb, skipping the record")(e)
        IO.pure(Left(ErrorDetails("Failed to write schedule to dynamoDB", OrchestrationError)))

      case NonFatal(e) =>
        warnWithException("Failed to write Kafka record to DynamoDb, failing the stream")(e)
        IO.raiseError(e)
    }
  }

  val removeSchedule = QuartzScheduling.removeSchedule _
  val descheduleComm = Scheduler.descheduleComm(schedulingPersistence.cancelSchedules, removeSchedule) _

  val triggeredV4Consumer: TriggeredV4 => IO[Unit] = {
    TriggeredConsumer[IO](
      orchestrateComm = orchestrateComm,
      scheduleTask = scheduleTask,
      sendFailedEvent = sendFailedEvent.andThen(_.map(_ => ())),
      generateTraceToken = () => UUID.randomUUID().toString,
      sendOrchestrationStartedEvent = sendOrchestrationStartedEvent
    )
  }

  val triggeredV3Consumer: TriggeredV3 => IO[Unit] = (triggeredV3: TriggeredV3) =>
    triggeredV4Consumer(triggeredV3.toV4)

  val cancellationRequestV3Consumer = {
    CancellationRequestConsumer(
      sendFailedCancellationEvent = sendFailedCancellationEvent,
      sendSuccessfulCancellationEvent = sendCancelledEvent,
      descheduleComm = descheduleComm,
      generateTraceToken = () => UUID.randomUUID().toString
    )
  }

  val cancellationRequestV2Consumer = (cancellationRequestedV2: CancellationRequestedV2) => {
    cancellationRequestV3Consumer(cancellationRequestedV2.toV3)
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
    def recordProcessor[T: Loggable, Result](consume: T => IO[Result]): Record[T] => IO[Unit] = { record: Record[T] =>
      record.value() match {
        case Some(r) =>
          IO(info((record, r))("Consumed Kafka Message")) >> consume(r) >> IO.pure(())
        case None =>
          IO(fail(record)(s"Skipping event: $record, failed to parse"))
      }
    }

    val triggeredV3Stream = KafkaConsumer[IO](topic = triggeredV3Topic)(f = recordProcessor(triggeredV3Consumer))
    val triggeredV4Stream = KafkaConsumer[IO](topic = triggeredV4Topic)(f = recordProcessor(triggeredV4Consumer))

    val cancellationRequestV2Stream =
      KafkaConsumer[IO](topic = cancellationRequestedV2Topic)(f = recordProcessor(cancellationRequestV2Consumer))
    val cancellationRequestV3Stream =
      KafkaConsumer[IO](topic = cancellationRequestedV3Topic)(f = recordProcessor(cancellationRequestV3Consumer))

    val s = Stream.eval(IO(info("Orchestration started"))) >>
        triggeredV3Stream
          .mergeHaltBoth(triggeredV4Stream)
          .mergeHaltBoth(cancellationRequestV2Stream)
          .mergeHaltBoth(cancellationRequestV3Stream)

    s.drain.covaryOutput[StreamApp.ExitCode] ++ Stream.emit(StreamApp.ExitCode.Error)
  }
}
