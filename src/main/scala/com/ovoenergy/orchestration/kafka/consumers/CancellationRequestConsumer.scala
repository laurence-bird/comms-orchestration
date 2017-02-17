package com.ovoenergy.orchestration.kafka.consumers

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.kafka.{KafkaConfig, Serialisation}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.control.NonFatal

object CancellationRequestConsumer extends LoggingWithMDC {
  override def loggerName: String = "CancellationRequestGraph"

  val deserializer = Serialisation.cancellationRequestedDeserializer

  def apply(
             sendFailedCancellationEvent: (FailedCancellation) => Future[_],
             sendSuccessfulCancellationEvent: (Cancelled => Future[RecordMetadata]),
             descheduleComm: CancellationRequested => Seq[Either[ErrorDetails, Metadata]],
             config: KafkaConfig,
             generateTraceToken: () => String)
           (implicit actorSystem: ActorSystem, materializer: Materializer): RunnableGraph[Control] = {

    implicit val executionContext = actorSystem.dispatcher

    val decider: Supervision.Decider = {
      case NonFatal(e) =>
        log.error("Stopping due to error", e)
        Supervision.Stop
    }

    val consumerSettings =
      ConsumerSettings(actorSystem, new StringDeserializer, deserializer)
        .withBootstrapServers(config.hosts)
        .withGroupId(config.groupId)

    val source = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(config.topic))
      .mapAsync(1)(msg => {
        log.debug(s"Event received $msg")
        val result: Future[_] = msg.record.value match {
          case Some(cancellationRequest) =>
            val futures = descheduleComm(cancellationRequest).map {
              case Left(err) =>
                sendFailedCancellationEvent(FailedCancellation(cancellationRequest, s"Cancellation of scheduled comm failed: ${err.reason}"))
              case Right(metadata) =>
                sendSuccessfulCancellationEvent(Cancelled(metadata))
            }
            Future.sequence(futures)
          case None =>
            log.warn(s"Skipping event: $msg, failed to parse")
            Future.successful(())
        }
        result
          .flatMap(_ => msg.committableOffset.commitScaladsl())
      }).withAttributes(ActorAttributes.supervisionStrategy(decider))

    val sink = Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider))

    log.debug(s"Consuming cancellation requests for: $config")
    source.to(sink)
  }
}
