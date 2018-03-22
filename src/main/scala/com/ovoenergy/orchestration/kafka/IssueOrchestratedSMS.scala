package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import com.ovoenergy.orchestration.kafka.producers.Publisher
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedSMS(producer: Publisher[OrchestratedSMSV2]) extends IssueOrchestratedComm[MobilePhoneNumber] {

  def send[F[_]: Async](customerProfile: Option[CustomerProfile],
                        mobileNumber: MobilePhoneNumber,
                        triggered: TriggeredV3) = {
    val orchestratedSMSEvent = OrchestratedSMSV2(
      metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      recipientPhoneNumber = mobileNumber.number
    )
    producer.publish(orchestratedSMSEvent, orchestratedSMSEvent.metadata.eventId)
  }
}
