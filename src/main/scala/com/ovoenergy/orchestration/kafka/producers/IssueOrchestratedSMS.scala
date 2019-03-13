package com.ovoenergy.orchestration
package kafka
package producers

import java.util.UUID

import cats.effect.{Async, IO}

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.sms.OrchestratedSMSV3
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import org.apache.kafka.clients.producer.RecordMetadata

object IssueOrchestratedSMS {

  def apply[F[_]: Async](config: Config.Kafka,
                         topic: Config.Topic[OrchestratedSMSV3]): IssueOrchestratedComm[F, MobilePhoneNumber] =
    new IssueOrchestratedComm[F, MobilePhoneNumber] {
      val produceOrchestratedSMSEvent =
        Producer.publisherFor[OrchestratedSMSV3, F](config, topic, _.metadata.commId)

      override def send(customerProfile: Option[CustomerProfile],
                        mobileNumber: MobilePhoneNumber,
                        triggered: TriggeredV4): F[RecordMetadata] = {
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
}
