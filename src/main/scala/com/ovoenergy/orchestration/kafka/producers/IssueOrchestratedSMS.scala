package com.ovoenergy.orchestration.kafka.producers

import java.util.UUID

import cats.effect.IO
import com.ovoenergy.comms.helpers.{Kafka, Topic}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.sms.OrchestratedSMSV3
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedSMS(topic: Topic[OrchestratedSMSV3])
    extends IssueOrchestratedComm[MobilePhoneNumber] {

  val produceOrchestratedSMSEvent =
    Producer.publisherFor[OrchestratedSMSV3](Kafka.aiven.orchestratedSMS.v3, _.metadata.commId)

  def send(customerProfile: Option[CustomerProfile], mobileNumber: MobilePhoneNumber, triggered: TriggeredV4): IO[RecordMetadata] = {
    val orchestratedSMSEvent = OrchestratedSMSV3(
      metadata = MetadataV3.fromSourceMetadata(
        "orchestration",
        triggered.metadata,
        triggered.metadata.commId ++ "-orchestrated"
      ),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      recipientPhoneNumber = mobileNumber.number
    )
    produceOrchestratedSMSEvent(orchestratedSMSEvent)
  }
}
