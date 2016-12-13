package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.{ChannelSelector, Orchestrator}
import com.ovoenergy.orchestration.processes.email.EmailOrchestration
import com.ovoenergy.orchestration.profile.CustomerProfiler
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile
import com.typesafe.config.ConfigFactory

import collection.JavaConverters._
import scala.concurrent.Future

object Main extends App
  with LoggingWithMDC {

  override def loggerName = "Main"

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  val config = ConfigFactory.load()

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = actorSystem.dispatcher

  val emailOrchestrator = EmailOrchestration(OrchestratedEmailProducer(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.email.orchestrated")
  ))

  val orchestrator = Orchestrator(
    emailOrchestrator = emailOrchestrator
  )

  OrchestrationFlow(
    consumerDeserializer = Serialisation.triggeredDeserializer,
    commOrchestrator = orchestrator,
    config = OrchestrationFlowConfig(
      hosts = config.getString("kafka.hosts"),
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered")
    )
  )

  log.info("Orchestration started")
}
