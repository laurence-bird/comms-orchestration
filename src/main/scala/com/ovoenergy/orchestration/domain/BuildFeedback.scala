package com.ovoenergy.orchestration.domain

import java.time.Instant

import com.ovoenergy.comms.model.{Customer, DeliverTo, Feedback, FeedbackOptions}
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.kafka.common.event.EventMetadata

trait BuildFeedback[T] {
  def apply(t: T): Feedback
}

object BuildFeedback {

  def instance[T](f: T => Feedback) = {
    new BuildFeedback[T] {
      override def apply(t: T): Feedback = f(t)
    }
  }

  def extractCustomer(deliverTo: DeliverTo): Option[Customer] = {
    deliverTo match {
      case customer: Customer => Some(customer)
      case _                  => None
    }
  }

  implicit val buildFeedbackId = instance[Feedback](identity)

  implicit val buildFeedbackErrorDetails = instance[FailureDetails] { fd =>
    val status = fd.failureType match {
      case InternalFailure     => FeedbackOptions.Failed
      case CancellationFailure => FeedbackOptions.FailedCancellation
    }

    Feedback(
      fd.commId.value,
      extractCustomer(fd.deliverTo),
      status,
      Some(fd.reason),
      None,
      None,
      EventMetadata(fd.traceToken.value, Hash(fd.eventId.value), Instant.now())
    )
  }
}
