package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.Sync
import cats.implicits._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.kafka.producers.IssueFeedback
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

object CancellationRequestConsumer extends LoggingWithMDC {

  def apply[F[_]](sendFailedCancellationEvent: FailedCancellationV3 => F[RecordMetadata],
                  sendSuccessfulCancellationEvent: CancelledV3 => F[RecordMetadata],
                  descheduleComm: CancellationRequestedV3 => List[Either[ErrorDetails, MetadataV3]],
                  generateTraceToken: () => String,
                  issueFeedback: IssueFeedback[F])(implicit ec: ExecutionContext,
                                                   F: Sync[F]): CancellationRequestedV3 => F[Unit] = {

    cancellationRequest: CancellationRequestedV3 =>
      {

        F.delay(info(cancellationRequest)(s"Event received: ${cancellationRequest.loggableString}")) *>
          descheduleComm(cancellationRequest).traverse_ {
            case Left(err) =>
              val failedCancellation = FailedCancellationV3(
                GenericMetadataV3.fromSourceGenericMetadata("orchestration", cancellationRequest.metadata),
                cancellationRequest,
                s"Cancellation of scheduled comm failed: ${err.reason}"
              )
              F.delay(warn(cancellationRequest)(s"Cancellation request failed with error $err")) *>
                sendFailedCancellationEvent(failedCancellation) *> issueFeedback.send(failedCancellation)
            case Right(metadata) =>
              val cancelledEvent = CancelledV3(MetadataV3.fromSourceMetadata(
                                                 "orchestration",
                                                 metadata,
                                                 metadata.commId ++ "-cancelled"
                                               ),
                                               cancellationRequest)
              sendSuccessfulCancellationEvent(cancelledEvent) *> issueFeedback.send(cancelledEvent)
          }
      }
  }
}
