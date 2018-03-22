package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.EmailAddress
import com.ovoenergy.orchestration.kafka.producers.Publisher
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedEmail(producer: Publisher[OrchestratedEmailV3]) extends IssueOrchestratedComm[EmailAddress] {

  def send[F[_]: Async](customerProfile: Option[CustomerProfile],
                        emailAddress: EmailAddress,
                        triggered: TriggeredV3): F[Either[String, Unit]] = {
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

    producer.publish(orchestratedEmailEvent, orchestratedEmailEvent.metadata.eventId)
  }

}
