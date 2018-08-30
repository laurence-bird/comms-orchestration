package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model.{Customer, DeliverTo, Feedback, FeedbackOptions}
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.kafka.common.event.EventMetadata
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

trait BuildFeedback[T] {
  def apply(t: T): Feedback
}

object BuildFeedback{

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


  implicit val buildFeedbackId          = instance[Feedback](identity)
  implicit val buildFeedbackErrorDetails = instance[FailureDetails]{ fd =>
    Feedback(
      fd.metadata.commId,
      extractCustomer(fd.metadata.deliverTo),
      FeedbackOptions.Failed,
      Some(fd.reason),
      None,
      None,
      EventMetadata.fromMetadata(fd.metadata, Hash(fd.metadata.eventId))
    )
  }
}
