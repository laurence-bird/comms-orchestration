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
import com.ovoenergy.orchestration.domain._
import com.ovoenergy.orchestration.kafka.Serialisation
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

object LegacyCancellationRequestConsumer extends LoggingWithMDC {

  val deserializer = Serialisation.legacyCancellationRequestedDeserializer

  def apply(topic: Topic[CancellationRequested],
            sendFailedCancellationEvent: (FailedCancellationV2) => Future[RecordMetadata],
            sendSuccessfulCancellationEvent: (CancelledV2 => Future[RecordMetadata]),
            descheduleComm: CancellationRequestedV2 => Seq[Either[ErrorDetails, MetadataV2]],
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
        log.debug(s"Event received $msg")
        val result: Future[Seq[RecordMetadata]] = msg.record.value match {
          case Some(legacyCancellationRequest) =>
            val cancellationRequest = cancellationRequestedToV2(legacyCancellationRequest)
            val futures = descheduleComm(cancellationRequest).map {
              case Left(err) =>
                sendFailedCancellationEvent(
                  FailedCancellationV2(
                    GenericMetadataV2.fromSourceGenericMetadata("orchestration", cancellationRequest.metadata),
                    cancellationRequest,
                    s"Cancellation of scheduled comm failed: ${err.reason}"
                  ))
              case Right(metadata) =>
                sendSuccessfulCancellationEvent(
                  CancelledV2(MetadataV2.fromSourceMetadata("orchestration", metadata), cancellationRequest))
            }
            Future.sequence(futures)
          case None =>
            log.warn(s"Skipping event: $msg, failed to parse")
            Future.successful(Nil)
        }
        result
          .flatMap(_ => msg.committableOffset.commitScaladsl())
      })
      .withAttributes(ActorAttributes.supervisionStrategy(decider))

    val sink = Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider))

    log.debug(s"Consuming cancellation requests for: ${topic.name}")
    source.to(sink)
  }
}
