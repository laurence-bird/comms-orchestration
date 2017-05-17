package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.email.{OrchestratedEmailV2, OrchestratedEmailV3}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.CustomerDeliveryDetails
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedEmailEvent {

  def send(sendEvent: OrchestratedEmailV3 => Future[RecordMetadata]) = {
    (customerDeliveryDetails: CustomerDeliveryDetails, triggered: TriggeredV3, internalMetadata: InternalMetadata) =>
      val customerProfile = for {
        firstName <- customerDeliveryDetails.name.map(_.firstName)
        lastName  <- customerDeliveryDetails.name.map(_.lastName)
      } yield CustomerProfile(firstName, lastName)

      val orchestratedEmailEvent = OrchestratedEmailV3(
        metadata = MetadataV2.fromSourceMetadata(
          source = "orchestration",
          sourceMetadata = triggered.metadata
        ),
        recipientEmailAddress = customerDeliveryDetails.deliverTo,
        templateData = triggered.templateData,
        internalMetadata = internalMetadata,
        expireAt = triggered.expireAt,
        customerProfile = customerProfile
      )

      sendEvent(orchestratedEmailEvent)
  }

}
