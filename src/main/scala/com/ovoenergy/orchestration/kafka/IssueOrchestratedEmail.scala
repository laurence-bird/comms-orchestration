package com.ovoenergy.orchestration.kafka

import java.util.UUID

import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.EmailAddress
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedEmail(sendEvent: OrchestratedEmailV3 => Future[RecordMetadata])
    extends IssueOrchestratedComm[EmailAddress] {

  def send(customerProfile: Option[CustomerProfile],
           emailAddress: EmailAddress,
           triggered: TriggeredV3): Future[RecordMetadata] = {
    val orchestratedEmailEvent = OrchestratedEmailV3(
      metadata = MetadataV2.fromSourceMetadata(
        source = "orchestration",
        sourceMetadata = triggered.metadata
      ),
      recipientEmailAddress = emailAddress.address,
      templateData = triggered.templateData,
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      expireAt = triggered.expireAt,
      customerProfile = customerProfile
    )

    sendEvent(orchestratedEmailEvent)
  }

}
