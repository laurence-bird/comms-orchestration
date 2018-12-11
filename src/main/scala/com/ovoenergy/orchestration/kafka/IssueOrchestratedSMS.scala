package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model.sms.OrchestratedSMSV3
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedSMS[F[_]: Async](sendEvent: OrchestratedSMSV3 => F[RecordMetadata])
    extends IssueOrchestratedComm[MobilePhoneNumber, F] {

  def send(customerProfile: Option[CustomerProfile], mobileNumber: MobilePhoneNumber, triggered: TriggeredV4) = {
    val orchestratedSMSEvent = OrchestratedSMSV3(
      metadata = MetadataV3.fromSourceMetadata(
        "orchestration",
        triggered.metadata,
        Hash(triggered.metadata.eventId ++ "-orchestrated-sms")
      ),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      recipientPhoneNumber = mobileNumber.number
    )
    sendEvent(orchestratedSMSEvent)
  }
}
