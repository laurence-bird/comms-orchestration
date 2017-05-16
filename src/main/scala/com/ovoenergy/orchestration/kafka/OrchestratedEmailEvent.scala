package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.email.{OrchestratedEmailV2, OrchestratedEmailV3}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedEmailEvent {

  def send(sendEvent: OrchestratedEmailV3 => Future[RecordMetadata]) = {
    (customerDeliveryDetails: CustomerDeliveryDetails, triggered: TriggeredV3, internalMetadata: InternalMetadata) =>
      val orchestratedEmailEvent = OrchestratedEmailV3(
        metadata = MetadataV2.fromSourceMetadata(
          source = "orchestration",
          sourceMetadata = triggered.metadata
        ),
        recipientEmailAddress = customerDeliveryDetails.deliverTo,
        //TODO - Handle ContactDetails
        customerProfile = Some(
          CustomerProfile(firstName = customerDeliveryDetails.name.firstName,
                          lastName = customerDeliveryDetails.name.lastName)),
        templateData = triggered.templateData,
        internalMetadata = internalMetadata,
        expireAt = triggered.expireAt
      )

      sendEvent(orchestratedEmailEvent)
  }

}
