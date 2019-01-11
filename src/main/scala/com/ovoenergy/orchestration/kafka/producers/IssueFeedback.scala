package com.ovoenergy.orchestration.kafka.producers

import cats.Apply
import cats.effect.IO
import cats.implicits._
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model.{FailedV3, Feedback, InternalMetadata, MetadataV3}
import com.ovoenergy.orchestration.domain.{BuildFeedback, FailureDetails}
import org.apache.kafka.clients.producer.RecordMetadata

class IssueFeedback(feedbackTopic: Topic[Feedback], failedTopic: Topic[FailedV3]) {

  val produceFeedback = Producer.publisherFor[Feedback](feedbackTopic, _.commId)
  val produceFailed   = Producer.publisherFor[FailedV3](failedTopic, _.metadata.commId)


  // TODO this should go away once we stop producing to old feedback topic
  def sendWithLegacy(failureDetails: FailureDetails,
                     metadata: MetadataV3,
                     internalMetadata: InternalMetadata): IO[RecordMetadata] = {
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
