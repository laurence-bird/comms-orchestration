package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.aws.AwsDynamoClientProvider
import com.ovoenergy.orchestration.http.HttpClient
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.kafka.consumers.{CancellationRequestConsumer, TriggeredConsumer}
import com.ovoenergy.orchestration.kafka.producers.{CancelledProducer, FailedProducer, OrchestratedEmailProducer}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.email.EmailOrchestration
import com.ovoenergy.orchestration.processes.{ChannelSelector, Orchestrator, Scheduler}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, Retry}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence
import com.ovoenergy.orchestration.scheduling.{QuartzScheduling, Restore, TaskExecutor}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.RecordMetadata

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

object Main extends App with LoggingWithMDC {

  override def loggerName = "Main"

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: JDuration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  val config = ConfigFactory.load()

  implicit val actorSystem      = ActorSystem()
  implicit val materializer     = ActorMaterializer()
  implicit val executionContext = actorSystem.dispatcher

  val region             = Regions.fromName(config.getString("aws.region"))
  val isRunningInCompose = sys.env.get("DOCKER_COMPOSE").contains("true")

  val schedulingPersistence = new DynamoPersistence(
    orchestrationExpiryMinutes = config.getInt("scheduling.orchestration.expiry"),
    context = DynamoPersistence.Context(
      db = AwsDynamoClientProvider(isRunningInCompose, region),
      tableName = config.getString("scheduling.persistence.table")
    )
  )

  val determineChannel = ChannelSelector.determineChannel _

  val orchestrateEmail = EmailOrchestration(
    OrchestratedEmailProducer(
      hosts = config.getString("kafka.hosts"),
      topic = config.getString("kafka.topics.orchestrated.email.v2")
    )) _

  val profileCustomer = {
    val retryConfig = Retry.RetryConfig(
      attempts = config.getInt("profile.service.http.attempts"),
      backoff = Retry.Backoff.constantDelay(config.getDuration("profile.service.http.interval").toFiniteDuration)
    )
    CustomerProfiler(
      httpClient = HttpClient.apply,
      profileApiKey = config.getString("profile.service.apiKey"),
      profileHost = config.getString("profile.service.host"),
      retryConfig = retryConfig
    ) _
  }

  val orchestrateComm = Orchestrator(
    profileCustomer = profileCustomer,
    determineChannel = determineChannel,
    orchestrateEmail = orchestrateEmail
  ) _

  val sendFailedTriggerEvent: (String, TriggeredV2, ErrorCode, InternalMetadata) => Future[RecordMetadata] =
    FailedProducer.failedTrigger(
      hosts = config.getString("kafka.hosts"),
      topic = config.getString("kafka.topics.failed")
    ) _

  val sendCancelledEvent: (Cancelled) => Future[RecordMetadata] = CancelledProducer(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.scheduling.cancelled")
  )

  val sendFailedCancellationEvent: (FailedCancellation) => Future[_] = FailedProducer.failedCancellation(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.scheduling.failedCancellation")
  )

  val executeScheduledTask = TaskExecutor.execute(schedulingPersistence,
                                                  orchestrateComm,
                                                  () => UUID.randomUUID.toString,
                                                  sendFailedTriggerEvent) _
  val addSchedule    = QuartzScheduling.addSchedule(executeScheduledTask) _
  val scheduleTask   = Scheduler.scheduleComm(schedulingPersistence.storeSchedule _, addSchedule) _
  val removeSchedule = QuartzScheduling.removeSchedule _
  val descheduleComm = Scheduler.descheduleComm(schedulingPersistence.cancelSchedules, removeSchedule) _

  val schedulingGraph = TriggeredConsumer(
    consumerDeserializer = Serialisation.triggeredV2Deserializer,
    scheduleTask = scheduleTask,
    sendFailedEvent = sendFailedTriggerEvent,
    config = KafkaConfig(
      hosts = config.getString("kafka.hosts"),
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered.v2")
    ),
    generateTraceToken = () => UUID.randomUUID().toString
  )

  val cancellationRequestGraph: RunnableGraph[Control] = CancellationRequestConsumer(
    sendFailedCancellationEvent = sendFailedCancellationEvent,
    sendSuccessfulCancellationEvent = sendCancelledEvent,
    generateTraceToken = () => UUID.randomUUID().toString,
    descheduleComm = descheduleComm,
    config = KafkaConfig(
      hosts = config.getString("kafka.hosts"),
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
