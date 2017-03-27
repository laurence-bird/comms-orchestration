package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{InternalMetadata, Metadata, OrchestratedSMS, TriggeredV2}
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedSmsEvent {

  def send(sendEvent: OrchestratedSMS => Future[RecordMetadata])(customerProfile: CustomerDeliveryDetails,
                                                                 triggered: TriggeredV2,
                                                                 internalMetadata: InternalMetadata) {

    val orchestratedSMSEvent = OrchestratedSMS(
      metadata = Metadata.fromSourceMetadata(
        source = "orchestration",
        sourceMetadata = triggered.metadata
      ),
      customerProfile =
        model.CustomerProfile(firstName = customerProfile.name.firstName, lastName = customerProfile.name.lastName),
      templateData = triggered.templateData,
      internalMetadata = internalMetadata,
      expireAt = triggered.expireAt,
      recipientPhoneNumber = customerProfile.deliverTo
    )

    sendEvent(orchestratedSMSEvent)
  }
}
