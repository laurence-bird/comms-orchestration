package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.{ExecutionContext, Future}

object CancellationRequestConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](
      sendFailedCancellationEvent: (FailedCancellationV2) => F[RecordMetadata],
      sendSuccessfulCancellationEvent: (CancelledV2 => F[RecordMetadata]),
      descheduleComm: CancellationRequestedV2 => Seq[Either[ErrorDetails, MetadataV2]],
      generateTraceToken: () => String)(implicit ec: ExecutionContext): CancellationRequestedV2 => F[Unit] = {

    cancellationRequest: CancellationRequestedV2 =>
      {

        info(cancellationRequest)(s"Event received: ${cancellationRequest.loggableString}")

        val result: Seq[F[RecordMetadata]] = descheduleComm(cancellationRequest).map {
          case Left(err) =>
            warn(cancellationRequest)(s"Cancellation request failed with error $err")
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

        import cats.implicits._

        result.toList.sequence
          .map(_ => ())
      }
  }
}
