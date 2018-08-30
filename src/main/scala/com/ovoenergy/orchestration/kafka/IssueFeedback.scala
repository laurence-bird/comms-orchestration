package com.ovoenergy.orchestration.kafka

import cats.effect.Async
import com.ovoenergy.comms.model.Feedback
import org.apache.kafka.clients.producer.RecordMetadata

// Option 2: Follow same pattern as before
class IssueFeedback[F[_]: Async](sendEvent: Feedback => F[RecordMetadata]){

  def send[SomeStuff](someStuff: SomeStuff): F[RecordMetadata] = {
    val feedback: Feedback = ???

    sendEvent(feedback)
  }
}
