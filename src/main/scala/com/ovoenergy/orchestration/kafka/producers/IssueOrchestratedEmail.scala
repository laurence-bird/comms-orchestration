package com.ovoenergy.orchestration.kafka.producers

import java.util.UUID

import cats.effect.{Async, IO}
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.orchestration.domain.EmailAddress
import org.apache.kafka.clients.producer.RecordMetadata

object IssueOrchestratedEmail {
  def apply[F[_]: Async](topic: Topic[OrchestratedEmailV4]): IssueOrchestratedComm[F, EmailAddress] =
    new IssueOrchestratedComm[F, EmailAddress] {
      val produceOrchestratedEmailEvent =
        Producer.publisherFor[OrchestratedEmailV4, F](topic, _.metadata.commId)

      override def send(customerProfile: Option[CustomerProfile],
                        emailAddress: EmailAddress,
                        triggered: TriggeredV4): F[RecordMetadata] = {
        val orchestratedEmailEvent = OrchestratedEmailV4(
          metadata = MetadataV3.fromSourceMetadata(
            source = "orchestration",
            sourceMetadata = triggered.metadata,
            eventId = triggered.metadata.commId ++ "-orchestrated"
          ),
          recipientEmailAddress = emailAddress.address,
          templateData = triggered.templateData,
          internalMetadata = InternalMetadata(UUID.randomUUID.toString),
          expireAt = triggered.expireAt,
          customerProfile = customerProfile
        )

        produceOrchestratedEmailEvent(orchestratedEmailEvent)
      }
    }
}
