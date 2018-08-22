package com.ovoenergy.comms.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.domain.EmailAddress
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedEmail[F[_]: Async](sendEvent: OrchestratedEmailV4 => F[RecordMetadata])
    extends IssueOrchestratedComm[EmailAddress, F] {

  def send(customerProfile: Option[CustomerProfile],
           emailAddress: EmailAddress,
           triggered: TriggeredV4): F[RecordMetadata] = {
    val orchestratedEmailEvent = OrchestratedEmailV4(
      metadata = MetadataV3.fromSourceMetadata(
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
