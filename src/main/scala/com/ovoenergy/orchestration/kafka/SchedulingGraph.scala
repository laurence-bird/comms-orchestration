package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{ErrorCode, InternalMetadata, TriggeredV2}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.ScheduleId
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.control.NonFatal

case class SchedulingGraphConfig(hosts: String, groupId: String, topic: String)

object SchedulingGraph extends LoggingWithMDC {
  override def loggerName: String = "OrchestrationGraph"

  def apply(consumerDeserializer: Deserializer[Option[TriggeredV2]],
            scheduleTask: (TriggeredV2) => Either[ErrorDetails, _],
            sendFailedEvent: (String, TriggeredV2, ErrorCode, InternalMetadata) => Future[_],
            config: SchedulingGraphConfig,
            generateTraceToken: () => String)
            (implicit actorSystem: ActorSystem, materializer: Materializer): RunnableGraph[Control] = {

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
      .mapAsync(1)(msg => {
        log.debug(s"Event received $msg")
        val result: Future[_] = msg.record.value match {
          case Some(triggered) =>
            scheduleTask(triggered) match {
              case Left(err) => sendFailedEvent(s"Scheduling of comm failed: ${err.reason}", triggered, err.errorCode, InternalMetadata(generateTraceToken()))
              case Right(_) => Future.successful(())
            }
          case None =>
            log.warn(s"Skipping event: $msg, failed to parse")
            Future.successful(())
        }
        result
          .flatMap(_ => msg.committableOffset.commitScaladsl())
      }).withAttributes(ActorAttributes.supervisionStrategy(decider))

    val sink = Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider))

    source.to(sink)
  }
}
