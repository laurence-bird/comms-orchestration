package com.ovoenergy.orchestration.kafka

import cats.effect.Async
import cats.syntax.flatMap._
import com.ovoenergy.comms.model.{FailedV3, Feedback}
import com.ovoenergy.orchestration.domain.{BuildFeedback, FailureDetails}
import org.apache.kafka.clients.producer.RecordMetadata

class IssueFeedback[F[_]: Async](sendFeedback: Feedback => F[RecordMetadata],
                                 sendFailed: FailedV3 => F[RecordMetadata]) {
  def send[T](t: T)(implicit buildFeedback: BuildFeedback[T]): F[RecordMetadata] = {
    val feedback: Feedback = buildFeedback(t)
    sendFeedback(feedback)
  }

  // TODO this should go away once we stop producing to old feedback topic
  def sendWithLegacy(failureDetails: FailureDetails): F[RecordMetadata] = {
    val feedback = BuildFeedback.buildFeedbackErrorDetails(failureDetails)
    val failed = FailedV3(
      failureDetails.metadata,
      failureDetails.internalMetadata,
      failureDetails.reason,
      failureDetails.errorCode
    )
    sendFeedback(feedback) >> sendFailed(failed)
  }
}
