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

  def instance[T](f: T => Feedback): BuildFeedback[T] = {
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
    // FIXME CancellationFailure is not a Status, it need to be modelled in another way
    val status = fd.failureType match {
      case InternalFailure     => FeedbackOptions.Failed
      case CancellationFailure => FeedbackOptions.FailedCancellation
    }

    // We are using the "-failed" suffix here as we should not send cancellation
    // failed as status
    Feedback(
      fd.commId.value,
      Some(fd.friendlyDescription),
      extractCustomer(fd.deliverTo),
      status,
      Some(fd.reason),
      None,
      None,
      Some(fd.templateManifest),
      EventMetadata(fd.traceToken.value, fd.commId.value ++ "-failed", Instant.now())
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
        EventMetadata.fromMetadata(os.metadata, os.metadata.commId ++ "-failed")
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
      EventMetadata.fromMetadata(cancelled.metadata, cancelled.metadata.commId ++ "-cancelled")
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
        EventMetadata.fromGenericMetadata(
          fc.metadata,
          fc.metadata.commId ++ "-failed-cancellation-" ++ fc.metadata.createdAt.toEpochMilli.toString)
      )

  }
}
