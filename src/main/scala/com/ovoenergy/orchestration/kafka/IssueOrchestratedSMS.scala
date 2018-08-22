package com.ovoenergy.comms.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.sms.OrchestratedSMSV3
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.domain.MobilePhoneNumber
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedSMS[F[_]: Async](sendEvent: OrchestratedSMSV3 => F[RecordMetadata])
    extends IssueOrchestratedComm[MobilePhoneNumber, F] {

  def send(customerProfile: Option[CustomerProfile], mobileNumber: MobilePhoneNumber, triggered: TriggeredV4) = {
    val orchestratedSMSEvent = OrchestratedSMSV3(
      metadata = MetadataV3.fromSourceMetadata("orchestration", triggered.metadata),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      recipientPhoneNumber = mobileNumber.number
    )
    sendEvent(orchestratedSMSEvent)
  }
}
