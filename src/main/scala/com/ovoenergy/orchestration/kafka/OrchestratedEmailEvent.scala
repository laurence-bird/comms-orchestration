package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{InternalMetadata, Metadata, OrchestratedEmailV2, TriggeredV2}
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedEmailEvent {

  def send(sendEvent: OrchestratedEmailV2 => Future[RecordMetadata])(customerDeliveryDetails: CustomerDeliveryDetails,
                                                                     triggered: TriggeredV2,
                                                                     internalMetadata: InternalMetadata) = {

    val orchestratedEmailEvent = OrchestratedEmailV2(
      metadata = Metadata.fromSourceMetadata(
        source = "orchestration",
        sourceMetadata = triggered.metadata
      ),
      recipientEmailAddress = customerDeliveryDetails.deliverTo,
      customerProfile = model.CustomerProfile(firstName = customerDeliveryDetails.name.firstName,
                                              lastName = customerDeliveryDetails.name.lastName),
      templateData = triggered.templateData,
      internalMetadata = internalMetadata,
      expireAt = triggered.expireAt
    )

    sendEvent(orchestratedEmailEvent)
  }

}
