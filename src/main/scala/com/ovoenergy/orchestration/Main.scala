package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import fs2._

import ciris.cats.effect._

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ProvisionedThroughputExceededException}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.{ErrorsOr, TemplateMetadataContext}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.kafka.consumers._
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.ovoenergy.orchestration.processes.{ChannelSelectorWithTemplate, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, ProfileValidation}
import com.ovoenergy.orchestration.scheduling.dynamo.{AsyncPersistence, DynamoPersistence}
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

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.internal.CredentialsEndpointProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBAsync,
  AmazonDynamoDBAsyncClientBuilder,
  AmazonDynamoDBClientBuilder
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ovoenergy.comms.templates.TemplatesContext
import org.slf4j.LoggerFactory

object Main extends IOApp with LoggingWithMDC with ExecutionContexts {

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit val ec: ExecutionContext = executionContext

  lazy val config: Config = Config.load[IO].orRaiseThrowable.unsafeRunSync()

  val dbClient = config.amazonDynamoDbClientBuilder.build()

  val dynamoContext = DynamoPersistence.Context(
    db = dbClient,
    tableName = config.schedulingTable
  )

  // Until we rethink our scheduling architecture, we need to use the bloking synchronous
  // db client for the task executor, due to multithreading hell
  val schedulingPersistence = new DynamoPersistence(
    orchestrationExpiry = config.schedulingExpiration,
    context = dynamoContext
  )

  val consumerSchedulingPersistence = new AsyncPersistence(
    context = dynamoContext,
    blockingExecutionContext
  )

  def retry: Retry[IO] = Retry.instance[IO](5.seconds, 5)

  val determineChannel = {
    val templatesContext        = TemplatesContext.cachingContext(new DefaultAWSCredentialsProviderChain)
    val templateMetadataContext = TemplateMetadataContext(dynamoContext.db, config.templateSummaryTable)
    val retrieveTemplateDetails = RetrieveTemplateDetails[IO](
      templatesContext,
      templateMetadataContext,
      CachingStrategy.caffeine[TemplateId, ErrorsOr[CommType]](250),
      blockingExecutionContext,
      Retry.backOff())

    new ChannelSelectorWithTemplate(retrieveTemplateDetails)
  }

  val produceOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata] =
    Producer.publisherFor[OrchestrationStartedV3, IO](config.kafka, config.orchestrationStartedTopic, _.metadata.commId)
  val issueFeedback = IssueFeedback[IO](config.kafka, config.feedbackTopic, config.failedTopic)

  private val customerProfilerResource: Resource[IO, CustomerProfiler[IO]] =
    CustomerProfiler
      .resource[IO](
        profileUri = config.profilesEndpoint,
        // This means we spend a max of just over 2 minutes retrying,
        // which is hopefully long enough to recover from flaky DNS
        retry = Retry.backOff(
          8,
          1.second,
          2.0
        ),
        apiKey = config.profilesApiKey
      )

  // Currently this has a shared profiler which is used by quartz and the stream. This isn't being cleaned up properly
  val customerProfiler = customerProfilerResource.allocated.unsafeRunSync()._1

  val orchestrateComm = Orchestrator[IO](
    channelSelector = determineChannel,
    getValidatedCustomerProfile = ProfileValidation.getValidatedCustomerProfile(customerProfiler),
    getValidatedContactProfile = ProfileValidation.validateContactProfile,
    issueOrchestratedEmail = IssueOrchestratedEmail.apply[IO](config.kafka, config.orchestratedEmailTopic),
    issueOrchestratedSMS = IssueOrchestratedSMS.apply[IO](config.kafka, config.orchestratedSmsTopic),
    issueOrchestratedPrint = IssueOrchestratedPrint.apply[IO](config.kafka, config.orchestratedPrintTopic)
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

  val triggeredV4Consumer: TriggeredV4 => IO[Unit] = {
    TriggeredConsumer[IO](
      orchestrator = orchestrateComm,
      scheduleTask = scheduleTask,
      issueFeedback = issueFeedback,
      generateTraceToken = () => UUID.randomUUID().toString,
      sendOrchestrationStartedEvent = produceOrchestrationStartedEvent
    )
  }

  val schedulingStream = {

    val initShutdown = Stream.bracket(IO(QuartzScheduling.init()))(_ => IO(QuartzScheduling.shutdown()))

    /*
      For pending schedules, we only need to load them once at startup.
      Actually a few minutes after startup, as we need to wait until this instance
      is consuming Kafka events. If we load the pending schedules while a previous
      instance is still processing events, we might miss some.
     */

    val loadPendingStream = Stream
      .eval {
        IO(Restore.pickUpPendingSchedules(schedulingPersistence, addSchedule)).flatMap { scheduledCount =>
          IO(log.info(s"Loaded $scheduledCount pending schedules"))
        }
      }
      .delayBy(config.loadPendingDelay)

    /*
    For expired schedules, we poll every few minutes.
    They should be very rare. Expiry only happens when an instance starts orchestrating
    and then dies half way through.
     */
    val pollForExpiredStream = Stream
      .awakeDelay[IO](config.pollForExpiredInterval)
      .zip(Stream.repeatEval {
        IO(Restore.pickUpExpiredSchedules(schedulingPersistence, addSchedule)).flatMap { count =>
          IO(log.info(s"Recovered $count expired schedules"))
        }
      })
      .delayBy(config.pollForExpiredDelay)

    initShutdown.flatMap { _ =>
      Stream(
        pollForExpiredStream,
        loadPendingStream
      ).parJoinUnbounded
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {

    // TODO: Improve error handling here
    def recordProcessor[T: Loggable, Result](consume: T => IO[Result]): Record[T] => IO[Unit] = { record: Record[T] =>
      IO(info((record, record.value()))("Consumed Kafka Message")) >> consume(record.value) >> IO.pure(())
    }

    val triggeredV4Stream =
      KafkaConsumer[IO](config.kafka, config.triggeredTopics)()(f = recordProcessor(triggeredV4Consumer))

    Stream(
      triggeredV4Stream,
      schedulingStream,
      Stream.eval(IO(info("Orchestration started")))
    ).parJoinUnbounded.compile.drain
      .as(ExitCode.Success)
  }
}
