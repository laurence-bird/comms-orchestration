package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model.{CustomerProfile, Triggered}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.control.NonFatal

case class OrchestrationFlowConfig(hosts: String, groupId: String, topic: String)

object OrchestrationFlow extends LoggingWithMDC {
  override def loggerName: String = "OrchestrationFlow"

  def apply(consumerDeserializer: Deserializer[Option[Triggered]], commOrchestrator: (Triggered) => Future[_], config: OrchestrationFlowConfig)
              (implicit actorSystem: ActorSystem, materializer: Materializer) = {

    implicit val executionContext = actorSystem.dispatcher

    val decider: Supervision.Decider = {
      case NonFatal(e) =>
        log.error("Restarting due to error", e)
        Supervision.Restart
    }

    val consumerSettings =
      ConsumerSettings(actorSystem, new StringDeserializer, consumerDeserializer)
        .withBootstrapServers(config.hosts)
        .withGroupId(config.groupId)

    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(config.topic))
      .mapAsync(1)(msg => {
        log.debug(s"Event received $msg")
        val result = msg.record.value match {
          case Some(triggered) =>
            commOrchestrator(triggered)
          case None =>
            log.error(s"Skipping event: $msg, failed to parse")
            Future.successful(())
        }
        result.map(_ => msg.committableOffset.commitScaladsl())
      })
      .to(Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider)))
      .run
  }
}