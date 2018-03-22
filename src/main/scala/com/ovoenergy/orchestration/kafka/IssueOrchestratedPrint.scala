package com.ovoenergy.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.print.OrchestratedPrint
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.ContactAddress
import com.ovoenergy.orchestration.kafka.producers.Publisher

class IssueOrchestratedPrint(producer: Publisher[OrchestratedPrint]) extends IssueOrchestratedComm[ContactAddress] {

  override def send[F[_]: Async](customerProfile: Option[CustomerProfile],
                                 contactInfo: ContactAddress,
                                 triggered: TriggeredV3): F[Either[String, Unit]] = {

    val orchestratedPrintEvent = OrchestratedPrint(
      metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      expireAt = triggered.expireAt,
      address = CustomerAddress(contactInfo.line1,
                                contactInfo.line2,
                                contactInfo.town,
                                contactInfo.county,
                                contactInfo.postcode,
                                contactInfo.country)
    )

    producer.publish(orchestratedPrintEvent, orchestratedPrintEvent.metadata.eventId)
  }
}
