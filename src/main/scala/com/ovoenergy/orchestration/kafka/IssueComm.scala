package com.ovoenergy.orchestration.kafka

import cats.effect.Async
import org.apache.kafka.clients.producer.RecordMetadata

trait Convert[T, Event]{
  def apply(t: T): Event
}


// Option 1: Remove IssueXXX, and use type class to convert between internal model and Event, and produce

class IssueComm[F[_]: Async, Event](sendEvent: Event => F[RecordMetadata]) {
  def send[A](a: A)(implicit convert: Convert[A, Event]) = {
    sendEvent(
      convert.apply(a)
    )
  }
}
