package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model._
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedEmailEvent {

  def send(sendEvent: OrchestratedEmailV3 => Future[RecordMetadata]) = {
    (customerProfile: Option[CustomerProfile], deliverTo: String, triggered: TriggeredV3,
     internalMetadata: InternalMetadata) =>
      val orchestratedEmailEvent = OrchestratedEmailV3(
        metadata = MetadataV2.fromSourceMetadata(
          source = "orchestration",
          sourceMetadata = triggered.metadata
        ),
        recipientEmailAddress = deliverTo,
        templateData = triggered.templateData,
        internalMetadata = internalMetadata,
        expireAt = triggered.expireAt,
        customerProfile = customerProfile
      )

      sendEvent(orchestratedEmailEvent)
  }

}
