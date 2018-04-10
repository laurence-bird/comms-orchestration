package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedSMS[F[_]: Async](sendEvent: OrchestratedSMSV2 => F[RecordMetadata])
    extends IssueOrchestratedComm[MobilePhoneNumber, F] {

  def send(customerProfile: Option[CustomerProfile], mobileNumber: MobilePhoneNumber, triggered: TriggeredV3) = {
    val orchestratedSMSEvent = OrchestratedSMSV2(
      metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      recipientPhoneNumber = mobileNumber.number
    )
    sendEvent(orchestratedSMSEvent)
  }
}
