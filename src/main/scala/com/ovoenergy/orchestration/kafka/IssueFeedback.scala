package com.ovoenergy.orchestration.kafka

import cats.effect.Async
import com.ovoenergy.comms.model.Feedback
import com.ovoenergy.orchestration.domain.BuildFeedback
import org.apache.kafka.clients.producer.RecordMetadata

class IssueFeedback[F[_]: Async](sendEvent: Feedback => F[RecordMetadata]) {
  def send[T](t: T)(implicit buildFeedback: BuildFeedback[T]): F[RecordMetadata] = {
    val feedback: Feedback = buildFeedback(t)
    // TODO: Send legacy Failed event in case of Failure
    sendEvent(feedback)
  }
}
