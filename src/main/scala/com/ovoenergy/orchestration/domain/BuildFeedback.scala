package com.ovoenergy.orchestration.domain

import java.time.Instant

import com.ovoenergy.comms.model.{
  CancelledV3,
  Customer,
  DeliverTo,
  FailedCancellationV3,
  Feedback,
  FeedbackOptions,
  OrchestrationStartedV3,
  TemplateManifest
}
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

  implicit val buildFeedbackId: BuildFeedback[Feedback] = instance[Feedback](identity)

  implicit val buildFeedbackErrorDetails: BuildFeedback[FailureDetails] = instance[FailureDetails] { fd =>
    val status = fd.failureType match {
      case InternalFailure     => FeedbackOptions.Failed
      case CancellationFailure => FeedbackOptions.FailedCancellation
    }

    Feedback(
      fd.commId.value,
      Some(fd.friendlyDescription),
      extractCustomer(fd.deliverTo),
      status,
      Some(fd.reason),
      None,
      None,
      Some(fd.templateManifest),
      EventMetadata(fd.traceToken.value, Hash(fd.eventId.value), Instant.now())
    )
  }

  implicit val buildFeedbackOrchestrationStarted: BuildFeedback[OrchestrationStartedV3] =
    instance[OrchestrationStartedV3] { os =>
      Feedback(
        os.metadata.commId,
        Some(os.metadata.friendlyDescription),
        extractCustomer(os.metadata.deliverTo),
        FeedbackOptions.Pending,
        Some(s"Trigger for communication accepted"),
        None,
        None,
        Some(TemplateManifest(os.metadata.templateManifest.id, os.metadata.templateManifest.version)),
        EventMetadata.fromMetadata(os.metadata, Hash(os.metadata.eventId))
      )
    }

  implicit val buildFeedbackCancelled: BuildFeedback[CancelledV3] = instance[CancelledV3] { cancelled =>
    Feedback(
      cancelled.metadata.commId,
      Some(cancelled.metadata.friendlyDescription),
      extractCustomer(cancelled.metadata.deliverTo),
      FeedbackOptions.Cancelled,
      Some(s"Scheduled comm has been cancelled"),
      None,
      None,
      Some(TemplateManifest(cancelled.metadata.templateManifest.id, cancelled.metadata.templateManifest.version)),
      EventMetadata.fromMetadata(cancelled.metadata, Hash(cancelled.metadata.eventId))
    )
  }
  implicit val buildFeedbackFailedCancellation: BuildFeedback[FailedCancellationV3] = instance[FailedCancellationV3] {
    fc =>
      Feedback(
        fc.metadata.commId,
        None,
        Some(Customer(fc.cancellationRequested.customerId)),
        FeedbackOptions.FailedCancellation,
        Some(fc.reason),
        None,
        None,
        None,
        EventMetadata.fromGenericMetadata(fc.metadata, s"failed-${fc.metadata.eventId}")
      )

  }
}