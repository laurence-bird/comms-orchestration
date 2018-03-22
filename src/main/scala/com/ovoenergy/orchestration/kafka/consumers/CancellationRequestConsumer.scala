package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.Async
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata
import cats.implicits._
import com.ovoenergy.orchestration.kafka.producers.Publisher
object CancellationRequestConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](sendFailedCancellationEvent: Publisher[FailedCancellationV2],
                         sendSuccessfulCancellationEvent: Publisher[CancelledV2],
                         descheduleComm: CancellationRequestedV2 => List[Either[ErrorDetails, MetadataV2]],
                         generateTraceToken: () => String)(cancellationRequest: CancellationRequestedV2): F[Unit] = {

    val results: List[F[Either[String, Unit]]] = descheduleComm(cancellationRequest).map {
      case Left(err) =>
        logWarn(cancellationRequest.metadata.traceToken, s"Cancellation request failed with error $err")
        sendFailedCancellationEvent.publish(
          FailedCancellationV2(
            GenericMetadataV2.fromSourceGenericMetadata("orchestration", cancellationRequest.metadata),
            cancellationRequest,
            s"Cancellation of scheduled comm failed: ${err.reason}"
          ),
          cancellationRequest.metadata.eventId
        )
      case Right(metadata) =>
        sendSuccessfulCancellationEvent.publish(
          CancelledV2(MetadataV2.fromSourceMetadata("orchestration", metadata), cancellationRequest),
          cancellationRequest.metadata.eventId)
    }

    results.sequence.flatMap { l =>
      val errors = l.collect {
        case Left(err) => err
      }
      // Fail if any errors are raised so we reprocess event
      if (errors.nonEmpty)
        ???
      else
        Async[F].pure(())
    }
  }
}
