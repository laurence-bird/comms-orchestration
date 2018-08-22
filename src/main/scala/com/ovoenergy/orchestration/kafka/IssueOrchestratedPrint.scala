package com.ovoenergy.comms.orchestration.kafka

import java.util.UUID

import cats.effect.Async
import com.ovoenergy.comms.model.print.OrchestratedPrintV2
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.domain.ContactAddress
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedPrint[F[_]: Async](sendEvent: OrchestratedPrintV2 => F[RecordMetadata])
    extends IssueOrchestratedComm[ContactAddress, F] {

  override def send(customerProfile: Option[CustomerProfile],
                    contactInfo: ContactAddress,
                    triggered: TriggeredV4): F[RecordMetadata] = {

    val orchestratedPrintEvent = OrchestratedPrintV2(
      metadata = MetadataV3.fromSourceMetadata("orchestration", triggered.metadata),
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

    sendEvent(orchestratedPrintEvent)
  }
}
