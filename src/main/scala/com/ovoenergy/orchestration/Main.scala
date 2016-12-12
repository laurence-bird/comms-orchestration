package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.kafka.{OrchestrationFlow, OrchestrationFlowConfig, Serialisation}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.ChannelSelector
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

  val emailOrchestrator = EmailOrchestration()

  def determineOrchestrator(channel: Channel): (CustomerProfile, Triggered) => Future[Done] = {
    channel match {
      case Email =>
        emailOrchestrator
      case _ =>
        (customerProfile: CustomerProfile, triggered: Triggered) =>
          logError(triggered.metadata.traceToken, s"Unsupported channel selected $channel")
          Future.successful(Done)
    }
  }

  def commsOrchestrator = (triggered: Triggered) => {
    for {
      customerProfile <- CustomerProfiler(triggered.metadata.customerId)
      channel <- ChannelSelector(customerProfile)
    } yield determineOrchestrator(channel)(customerProfile, triggered)
  }

  OrchestrationFlow(
    consumerDeserializer = Serialisation.triggeredDeserializer,
    commOrchestrator = commsOrchestrator,
    config = OrchestrationFlowConfig(
      hosts = config.getString("kafka.hosts"),
      groupId = config.getString("kafka.group.id"),
      topic = config.getString("kafka.topics.triggered")
    )
  )

  log.info("Orchestration started")
}
