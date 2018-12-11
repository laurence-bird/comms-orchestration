package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.domain.EmailAddress
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedEmail[F[_]: Async](sendEvent: OrchestratedEmailV4 => F[RecordMetadata])
    extends IssueOrchestratedComm[EmailAddress, F] {

  def send(customerProfile: Option[CustomerProfile],
           emailAddress: EmailAddress,
           triggered: TriggeredV4): F[RecordMetadata] = {
    val orchestratedEmailEvent = OrchestratedEmailV4(
      metadata = MetadataV3.fromSourceMetadata(
        source = "orchestration",
        sourceMetadata = triggered.metadata,
        eventId = Hash(triggered.metadata.eventId ++ "-orchestrated-email")
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
