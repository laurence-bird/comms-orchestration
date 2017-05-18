package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.comms.model._
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object OrchestratedSMSEvent {

  def send(sendEvent: OrchestratedSMSV2 => Future[RecordMetadata]) = {

    (customerProfile: Option[CustomerProfile], deliverTo: String, triggered: TriggeredV3,
     internalMetadata: InternalMetadata) =>
      val orchestratedSMSEvent = OrchestratedSMSV2(
        metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
        customerProfile = customerProfile,
        templateData = triggered.templateData,
        internalMetadata = internalMetadata,
        expireAt = triggered.expireAt,
        recipientPhoneNumber = deliverTo
      )
      sendEvent(orchestratedSMSEvent)
  }
}
