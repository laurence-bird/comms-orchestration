package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.Async
import cats.syntax.flatMap._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.kafka.common.event.EventMetadata
import com.ovoenergy.orchestration.domain.{CancellationFailure, CommId, EventId, FailureDetails, TraceToken}
import com.ovoenergy.orchestration.kafka.IssueFeedback
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

object CancellationRequestConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](
      sendFailedCancellationEvent: FailedCancellationV3 => F[RecordMetadata],
      sendSuccessfulCancellationEvent: CancelledV3 => F[RecordMetadata],
      descheduleComm: CancellationRequestedV3 => Seq[Either[ErrorDetails, MetadataV3]],
      generateTraceToken: () => String,
      issueFeedback: IssueFeedback[F])(implicit ec: ExecutionContext): CancellationRequestedV3 => F[Unit] = {

    cancellationRequest: CancellationRequestedV3 =>
      {

        info(cancellationRequest)(s"Event received: ${cancellationRequest.loggableString}")

        val result: Seq[F[RecordMetadata]] = descheduleComm(cancellationRequest).map {
          case Left(err) =>
            warn(cancellationRequest)(s"Cancellation request failed with error $err")
            val failureDetails = FailureDetails(
              Customer(cancellationRequest.customerId),
              cancellationRequest.metadata.commId,
              cancellationRequest.metadata.traceToken,
              cancellationRequest.metadata.eventId,
              s"Cancellation of scheduled comm failed: ${err.reason}",
              OrchestrationError,
              CancellationFailure
            )
            sendFailedCancellationEvent(
              FailedCancellationV3(
                GenericMetadataV3.fromSourceGenericMetadata("orchestration", cancellationRequest.metadata),
                cancellationRequest,
                s"Cancellation of scheduled comm failed: ${err.reason}"
              )) >> issueFeedback.send(failureDetails)
          case Right(metadata) =>
            sendSuccessfulCancellationEvent(
              CancelledV3(MetadataV3.fromSourceMetadata(
                            "orchestration",
                            metadata,
                            Hash(metadata.eventId)
                          ),
                          cancellationRequest)) >> issueFeedback.send(
              Feedback(
                cancellationRequest.metadata.commId,
                Some(Customer(cancellationRequest.customerId)),
                FeedbackOptions.Cancelled,
                Some("Scheduled Comm cancelled"),
                None,
                None,
                EventMetadata.fromMetadata(metadata, Hash(metadata.eventId))
              ))
        }

        import cats.implicits._

        result.toList.sequence
          .map(_ => ())
      }
  }
}
