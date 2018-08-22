package com.ovoenergy.comms.orchestration.kafka.consumers

import cats.effect.Async
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.logging.LoggingWithMDC
import com.ovoenergy.comms.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

object CancellationRequestConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](
      sendFailedCancellationEvent: (FailedCancellationV3) => F[RecordMetadata],
      sendSuccessfulCancellationEvent: (CancelledV3 => F[RecordMetadata]),
      descheduleComm: CancellationRequestedV3 => Seq[Either[ErrorDetails, MetadataV3]],
      generateTraceToken: () => String)(implicit ec: ExecutionContext): CancellationRequestedV3 => F[Unit] = {

    cancellationRequest: CancellationRequestedV3 =>
      {

        info(cancellationRequest)(s"Event received: ${cancellationRequest.loggableString}")

        val result: Seq[F[RecordMetadata]] = descheduleComm(cancellationRequest).map {
          case Left(err) =>
            warn(cancellationRequest)(s"Cancellation request failed with error $err")
            sendFailedCancellationEvent(
              FailedCancellationV3(
                GenericMetadataV3.fromSourceGenericMetadata("orchestration", cancellationRequest.metadata),
                cancellationRequest,
                s"Cancellation of scheduled comm failed: ${err.reason}"
              ))
          case Right(metadata) =>
            sendSuccessfulCancellationEvent(
              CancelledV3(MetadataV3.fromSourceMetadata("orchestration", metadata), cancellationRequest))
        }

        import cats.implicits._

        result.toList.sequence
          .map(_ => ())
      }
  }
}
