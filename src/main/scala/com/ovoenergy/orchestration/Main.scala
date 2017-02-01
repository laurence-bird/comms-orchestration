package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ovoenergy.comms.model.{TemplateData, Triggered, TriggeredV2}
import com.ovoenergy.orchestration.http.HttpClient
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.email.EmailOrchestration
import com.ovoenergy.orchestration.processes.failure.Failure
import com.ovoenergy.orchestration.processes.{ChannelSelector, Orchestrator}
import com.ovoenergy.orchestration.profile.{CustomerProfiler, Retry}
import com.typesafe.config.ConfigFactory
import shapeless.Coproduct

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object Main extends App
  with LoggingWithMDC {

  override def loggerName = "Main"

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  private implicit class RichDuration(val duration: Duration) extends AnyVal {
    def toFiniteDuration: FiniteDuration = FiniteDuration.apply(duration.toNanos, TimeUnit.NANOSECONDS)
  }

  val config = ConfigFactory.load()

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = actorSystem.dispatcher

  val channelSelector = ChannelSelector.determineChannel _

  val emailOrchestrator = EmailOrchestration(OrchestratedEmailProducer(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.orchestrated.email.v2")
  )) _

  val customerProfiler = {
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

  val orchestrator = Orchestrator(
    customerProfiler = customerProfiler,
    channelSelector = channelSelector,
    emailOrchestrator = emailOrchestrator
  ) _

  val failure = Failure(FailedProducer(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.failed")
  )) _

  val orchestrationGraph =  OrchestrationGraph(
    consumerDeserializer = Serialisation.triggeredV2Deserializer,
    orchestrationProcess = orchestrator,
    failureProcess = failure,
    config = OrchestrationGraphConfig(
      hosts = config.getString("kafka.hosts"),
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered.v2")
    ),
    traceTokenGenerator = () => UUID.randomUUID().toString
  )

  val control = orchestrationGraph.run()

  control.isShutdown.foreach { _ =>
    log.error("ARGH! The Kafka source has shut down. Killing the JVM and nuking from orbit.")
    System.exit(1)
  }

  // TODO: this whole block and the OrchestrationGraphV1 object can be removed once we have migrated all producers to TriggeredV2
  {
    val triggeredConverter = (t: Triggered) => TriggeredV2(
      metadata = t.metadata,
      templateData = t.templateData.mapValues(string => TemplateData(Coproduct[TemplateData.TD](string)))
    )

    val orchestrationGraphV1 = OrchestrationGraphV1(
      consumerDeserializer = Serialisation.triggeredDeserializer,
      triggeredConverter = triggeredConverter,
      orchestrationProcess = orchestrator,
      failureProcess = failure,
      config = OrchestrationGraphConfig(
        hosts = config.getString("kafka.hosts"),
        groupId = config.getString("kafka.group.id"),
        topic = config.getString("kafka.topics.triggered.v1")
      ),
      traceTokenGenerator = () => UUID.randomUUID().toString
    )

    val controlV1 = orchestrationGraphV1.run()

    controlV1.isShutdown.foreach { _ =>
      log.error("ARGH! The Kafka source has shut down (V1). Killing the JVM and nuking from orbit.")
      System.exit(1)
    }
  }

  log.info("Orchestration started")
}
