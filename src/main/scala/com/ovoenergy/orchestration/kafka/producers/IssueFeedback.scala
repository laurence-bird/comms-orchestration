package com.ovoenergy.orchestration.kafka.producers

import cats.effect.Async
import cats.implicits._
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model.{FailedV3, Feedback}
import com.ovoenergy.orchestration.domain.{BuildFeedback, FailureDetails}
import org.apache.kafka.clients.producer.RecordMetadata
import com.ovoenergy.comms.model._

trait IssueFeedback[F[_]] {
  def send[T](t: T)(implicit buildFeedback: BuildFeedback[T]): F[RecordMetadata]
  def sendWithLegacy(failureDetails: FailureDetails,
                     metadata: MetadataV3,
                     internalMetadata: InternalMetadata): F[RecordMetadata]
}

object IssueFeedback {
  def apply[F[_]: Async](feedbackTopic: Topic[Feedback], failedTopic: Topic[FailedV3]) = {
    new IssueFeedback[F] {
      val produceFeedback = Producer.publisherFor[Feedback, F](feedbackTopic, _.commId)
      val produceFailed   = Producer.publisherFor[FailedV3, F](failedTopic, _.metadata.commId)

      override def send[T](t: T)(implicit buildFeedback: BuildFeedback[T]): F[RecordMetadata] = {
        val feedback: Feedback = buildFeedback(t)
        produceFeedback(feedback)
      }

      // TODO this should go away once we stop producing to old feedback topic
      override def sendWithLegacy(failureDetails: FailureDetails,
                                  metadata: MetadataV3,
                                  internalMetadata: InternalMetadata): F[RecordMetadata] = {
        val feedback = BuildFeedback.buildFeedbackErrorDetails(failureDetails)

        val failed = FailedV3(
          MetadataV3.fromSourceMetadata("orchestrator", metadata, metadata.commId ++ "-failed"),
          internalMetadata,
          failureDetails.reason,
          failureDetails.errorCode
        )

        produceFeedback(feedback) *> produceFailed(failed)
      }
    }
  }
}
