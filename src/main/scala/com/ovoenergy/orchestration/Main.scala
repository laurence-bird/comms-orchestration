package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ProvisionedThroughputExceededException}
import com.ovoenergy.comms.helpers.{Kafka, Topic}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.{ErrorsOr, TemplateMetadataContext}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.kafka.consumers._
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.ovoenergy.orchestration.processes.{ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.scheduling.dynamo.{AsyncPersistence, DynamoPersistence}
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata
import fs2._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.ovoenergy.comms.templates.cache.CachingStrategy
import com.ovoenergy.comms.templates.model.template.metadata.TemplateId
import com.ovoenergy.orchestration.kafka.consumers.KafkaConsumer.Record
import com.ovoenergy.orchestration.kafka.producers._
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

  lazy val triggeredV4Topic: Topic[TriggeredV4]                         = Kafka.aiven.triggered.v4
  lazy val triggeredV4PriorityTopic: Topic[TriggeredV4]                 = Kafka.aiven.triggered.p0V4
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

  val produceCancelledEvent = Producer.publisherFor[CancelledV3, IO](Kafka.aiven.cancelled.v3, _.metadata.commId)
  val produceFailedCancellationEvent =
    Producer.publisherFor[FailedCancellationV3, IO](Kafka.aiven.failedCancellation.v3, _.metadata.commId)
  val produceOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata] =
    Producer.publisherFor[OrchestrationStartedV3, IO](Kafka.aiven.orchestrationStarted.v3, _.metadata.commId)
  val issueFeedback = IssueFeedback[IO](Kafka.aiven.feedback.v1, Kafka.aiven.failed.v3)

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
    issueOrchestratedEmail = IssueOrchestratedEmail.apply[IO](Kafka.aiven.orchestratedEmail.v4),
    issueOrchestratedSMS = IssueOrchestratedSMS.apply[IO](Kafka.aiven.orchestratedSMS.v3),
    issueOrchestratedPrint = IssueOrchestratedPrint.apply[IO](Kafka.aiven.orchestratedPrint.v2)
  )

  val executeScheduledTask = TaskExecutor.execute(schedulingPersistence,
                                                  orchestrateComm,
                                                  produceOrchestrationStartedEvent,
                                                  () => UUID.randomUUID.toString,
                                                  issueFeedback) _
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
      issueFeedback = issueFeedback,
      generateTraceToken = () => UUID.randomUUID().toString,
      sendOrchestrationStartedEvent = produceOrchestrationStartedEvent
    )
  }

  val cancellationRequestV3Consumer = {
    CancellationRequestConsumer(
      sendFailedCancellationEvent = produceFailedCancellationEvent,
      sendSuccessfulCancellationEvent = produceCancelledEvent,
      descheduleComm = descheduleComm,
      generateTraceToken = () => UUID.randomUUID().toString,
      issueFeedback = issueFeedback
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
    def recordProcessor[T: Loggable, Result](consume: T => IO[Result]): Record[T] => IO[Unit] = { record: Record[T] =>
      IO(info((record, record.value()))("Consumed Kafka Message")) >> consume(record.value) >> IO.pure(())
    }

    val triggeredV4Stream = KafkaConsumer[IO](topics = NonEmptyList.of(triggeredV4Topic, triggeredV4PriorityTopic))()(
      f = recordProcessor(triggeredV4Consumer))

    val cancellationRequestV3Stream =
      KafkaConsumer[IO](topics = NonEmptyList.of(cancellationRequestedV3Topic))()(
        f = recordProcessor(cancellationRequestV3Consumer))

    val s = Stream.eval(IO(info("Orchestration started"))) >>
        triggeredV4Stream
          .mergeHaltBoth(cancellationRequestV3Stream)

    s.drain.covaryOutput[StreamApp.ExitCode] ++ Stream.emit(StreamApp.ExitCode.Error)
  }
}
