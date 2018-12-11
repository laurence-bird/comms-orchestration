package com.ovoenergy.orchestration.kafka

import cats.Apply
import cats.implicits._
import com.ovoenergy.comms.model.{FailedV3, Feedback, InternalMetadata, MetadataV3}
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.domain.{BuildFeedback, FailureDetails}
import org.apache.kafka.clients.producer.RecordMetadata

class IssueFeedback[F[_]: Apply](sendFeedback: Feedback => F[RecordMetadata],
                                 sendFailed: FailedV3 => F[RecordMetadata]) {
  def send[T](t: T)(implicit buildFeedback: BuildFeedback[T]): F[RecordMetadata] = {
    val feedback: Feedback = buildFeedback(t)
    sendFeedback(feedback)
  }

  // TODO this should go away once we stop producing to old feedback topic
  def sendWithLegacy(failureDetails: FailureDetails,
                     metadata: MetadataV3,
                     internalMetadata: InternalMetadata): F[RecordMetadata] = {
    val feedback = BuildFeedback.buildFeedbackErrorDetails(failureDetails)
    val failed = FailedV3(
      MetadataV3.fromSourceMetadata("orchestrator", metadata, Hash(metadata.eventId ++ "-orchestration-feedback")),
      internalMetadata,
      failureDetails.reason,
      failureDetails.errorCode
    )

    sendFeedback(feedback) *> sendFailed(failed)
  }
}
