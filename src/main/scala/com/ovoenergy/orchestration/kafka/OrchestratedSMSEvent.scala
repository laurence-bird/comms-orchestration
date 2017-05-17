package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.sms.{OrchestratedSMS, OrchestratedSMSV2}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedSMSEvent {

  def send(sendEvent: OrchestratedSMSV2 => Future[RecordMetadata]) = {

    //TODO - Handle ContactDetails
    (customerProfile: CustomerDeliveryDetails, triggered: TriggeredV3, internalMetadata: InternalMetadata) =>
      val profile = for {
        firstName <- customerProfile.name.map(_.firstName)
        lastName  <- customerProfile.name.map(_.lastName)
      } yield CustomerProfile(firstName, lastName)

      val orchestratedSMSEvent = OrchestratedSMSV2(
        metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
        customerProfile = profile,
        templateData = triggered.templateData,
        internalMetadata = internalMetadata,
        expireAt = triggered.expireAt,
        recipientPhoneNumber = customerProfile.deliverTo
      )
      sendEvent(orchestratedSMSEvent)
  }
}
