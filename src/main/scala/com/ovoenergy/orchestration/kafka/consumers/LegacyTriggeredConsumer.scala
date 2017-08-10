package com.ovoenergy.orchestration.kafka.consumers

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.Subscriptions
import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.kafka.Serialisation
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.domain._
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal

object LegacyTriggeredConsumer extends LoggingWithMDC {

  val consumerDeserializer = Serialisation.legacyTriggeredDeserializer

  def apply(topic: Topic[TriggeredV2],
            scheduleTask: (TriggeredV3) => Either[ErrorDetails, Boolean],
            sendFailedEvent: FailedV2 => Future[RecordMetadata],
            generateTraceToken: () => String)(implicit actorSystem: ActorSystem,
                                              materializer: Materializer): RunnableGraph[Control] = {

    implicit val executionContext = actorSystem.dispatcher

    val decider: Supervision.Decider = {
      case NonFatal(e) =>
        log.error("Stopping due to error", e)
        Supervision.Stop
    }

    val source = Consumer
      .committableSource(topic.consumerSettings, Subscriptions.topics(topic.name))
      .throttle(5, 1.second, 10, Shaping)
      .mapAsync(1)(msg => {
        val result: Future[_] = msg.record.value match {
          case Some(triggered: TriggeredV2) =>
            val triggeredV3 = triggeredV2ToV3(triggered)
            scheduleTask(triggeredV3) match {
              case Left(err) =>
                sendFailedEvent(
                  FailedV2(triggeredV3.metadata,
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
    log.debug(s"Consuming triggered events for ${topic.name}")
    source.to(sink)
  }
}
