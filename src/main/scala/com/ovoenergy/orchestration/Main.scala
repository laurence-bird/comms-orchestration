package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ovoenergy.orchestration.kafka._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.email.EmailOrchestration
import com.ovoenergy.orchestration.processes.{ChannelSelector, Orchestrator}
import com.ovoenergy.orchestration.profile.CustomerProfiler
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

object Main extends App
  with LoggingWithMDC {

  override def loggerName = "Main"

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  val config = ConfigFactory.load()

  implicit val actorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = actorSystem.dispatcher

  val channelSelector = ChannelSelector.determineChannel

  val emailOrchestrator = EmailOrchestration(OrchestratedEmailProducer(
    hosts = config.getString("kafka.hosts"),
    topic = config.getString("kafka.topics.orchestrated.email")
  )) _

  val orchestrator = Orchestrator(
    customerProfiler = CustomerProfiler.apply,
    channelSelector = channelSelector,
    emailOrchestrator = emailOrchestrator
  ) _

  val orchestrationGraph =  OrchestrationGraph(
    consumerDeserializer = Serialisation.triggeredDeserializer,
    orchestrationProcess = orchestrator,
    config = OrchestrationGraphConfig(
      hosts = config.getString("kafka.hosts"),
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered")
    )
  )

  orchestrationGraph.run()

  log.info("Orchestration started")
}
