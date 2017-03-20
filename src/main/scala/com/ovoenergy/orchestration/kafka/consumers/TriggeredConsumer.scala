package com.ovoenergy.orchestration.kafka.consumers

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.kafka.KafkaConfig
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal

object TriggeredConsumer extends LoggingWithMDC {
  override def loggerName: String = "OrchestrationGraph"

  def apply(consumerDeserializer: Deserializer[Option[TriggeredV2]],
            scheduleTask: (TriggeredV2) => Either[ErrorDetails, Boolean],
            sendFailedEvent: Failed => Future[RecordMetadata],
            config: KafkaConfig,
            generateTraceToken: () => String)(implicit actorSystem: ActorSystem,
                                              materializer: Materializer): RunnableGraph[Control] = {

    implicit val executionContext = actorSystem.dispatcher

    val decider: Supervision.Decider = {
      case NonFatal(e) =>
        log.error("Stopping due to error", e)
        Supervision.Stop
    }

    val consumerSettings =
      ConsumerSettings(actorSystem, new StringDeserializer, consumerDeserializer)
        .withBootstrapServers(config.hosts)
        .withGroupId(config.groupId)

    val source = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(config.topic))
      .throttle(5, 1.second, 10, Shaping)
      .mapAsync(1)(msg => {
        val result: Future[_] = msg.record.value match {
          case Some(triggered) =>
            scheduleTask(triggered) match {
              case Left(err) =>
                sendFailedEvent(
                  Failed(triggered.metadata,
                         InternalMetadata(generateTraceToken()),
                         s"Scheduling of comm failed: ${err.reason}",
                         err.errorCode))
              case Right(_) => Future.successful(())
            }
          case None =>
            log.warn(s"Skipping event: $msg, failed to parse")
            Future.successful(())
        }
        result
          .flatMap(_ => msg.committableOffset.commitScaladsl())
      })
      .withAttributes(ActorAttributes.supervisionStrategy(decider))

    val sink = Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider))

    source.to(sink)
  }
}
