package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import fs2._
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
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.ovoenergy.comms.templates.cache.CachingStrategy
import com.ovoenergy.comms.templates.model.template.metadata.TemplateId
import com.ovoenergy.orchestration.kafka.consumers.KafkaConsumer.Record
import com.ovoenergy.orchestration.kafka.producers._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.ovoenergy.orchestration.templates.RetrieveTemplateDetails
import com.ovoenergy.orchestration.util.Retry
import org.http4s.Uri

import scala.util.control.NonFatal

object Main extends IOApp with LoggingWithMDC with ExecutionContexts {

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))
  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit val ec = executionContext

  implicit val config: Config                                           = ConfigFactory.load()
  implicit val actorSystem                                              = ActorSystem()
  implicit val materializer                                             = ActorMaterializer()
  lazy val kafka                                                        = Kafka.aiven
  lazy val triggeredV4Topic: Topic[TriggeredV4]                         = kafka.triggered.v4
  lazy val triggeredV4PriorityTopic: Topic[TriggeredV4]                 = kafka.triggered.p0V4
  lazy val cancellationRequestedV3Topic: Topic[CancellationRequestedV3] = kafka.cancellationRequested.v3
  lazy val kafkaConfig                                                  = kafka.kafkaConfig

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

  def retry: Retry[IO] = Retry.instance[IO](5.seconds, 5)

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
    CachingStrategy.caffeine[TemplateId, ErrorsOr[CommType]](250),
    Retry.backOff())
  val determineChannel = new ChannelSelectorWithTemplate(retrieveTemplateDetails)

  val produceCancelledEvent = Producer.publisherFor[CancelledV3, IO](Kafka.aiven.cancelled.v3, _.metadata.commId)
  val produceFailedCancellationEvent =
    Producer.publisherFor[FailedCancellationV3, IO](Kafka.aiven.failedCancellation.v3, _.metadata.commId)
  val produceOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata] =
    Producer.publisherFor[OrchestrationStartedV3, IO](Kafka.aiven.orchestrationStarted.v3, _.metadata.commId)
  val issueFeedback = IssueFeedback[IO](Kafka.aiven.feedback.v1, Kafka.aiven.failed.v3)

  private val customerProfilerResource: Resource[IO, CustomerProfiler[IO]] =
    CustomerProfiler
      .resource[IO](
        profileUri = Uri.unsafeFromString(config.getString("profile.service.host")),
        retry = Retry.backOff(
          config.getInt("profile.http.retry.attempts"),
          config.getDuration("profile.http.retry.initialInterval").toFiniteDuration,
          config.getDouble("profile.http.retry.exponent")
        ),
        apiKey = config.getString("profile.service.apiKey")
      )

  // Currently this has a shared profiler which is used by quartz and the stream. This isn't being cleaned up properly
  val customerProfiler = customerProfilerResource.allocated.unsafeRunSync()._1

  val orchestrateComm = Orchestrator[IO](
    channelSelector = determineChannel,
    getValidatedCustomerProfile = ProfileValidation.getValidatedCustomerProfile(customerProfiler),
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
    retry(
      Scheduler
        .scheduleComm(consumerSchedulingPersistence.storeSchedule[IO], addSchedule)
        .apply(t),
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
      orchestrator = orchestrateComm,
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

  override def run(args: List[String]): IO[ExitCode] = {
    // TODO: Improve error handling here
    def recordProcessor[T: Loggable, Result](consume: T => IO[Result]): Record[T] => IO[Unit] = { record: Record[T] =>
      IO(info((record, record.value()))("Consumed Kafka Message")) >> consume(record.value) >> IO.pure(())
    }

    val triggeredV4Stream = KafkaConsumer[IO](topics = NonEmptyList.of(triggeredV4Topic, triggeredV4PriorityTopic),
                                              kafkaConfig = kafkaConfig)()(f = recordProcessor(triggeredV4Consumer))

    val cancellationRequestV3Stream =
      KafkaConsumer[IO](topics = NonEmptyList.of(cancellationRequestedV3Topic), kafkaConfig = kafkaConfig)()(
        f = recordProcessor(cancellationRequestV3Consumer))

    Stream(
      triggeredV4Stream,
      cancellationRequestV3Stream,
      Stream.eval(IO(info("Orchestration started")))
    ).parJoinUnbounded.compile.drain
      .as(ExitCode.Success)
  }
}
