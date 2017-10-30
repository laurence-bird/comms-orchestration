package com.ovoenergy.orchestration.kafka

import java.util.UUID

import com.ovoenergy.comms.model.print.OrchestratedPrint
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.ContactAddress
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

class IssueOrchestratedPrint(sendEvent: OrchestratedPrint => Future[RecordMetadata])
    extends IssueOrchestratedComm[ContactAddress] {

  override def send(customerProfile: Option[CustomerProfile], contactInfo: ContactAddress, triggered: TriggeredV3) = {

    val orchestratedPrintEvent = OrchestratedPrint(
      metadata = MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
      internalMetadata = InternalMetadata(UUID.randomUUID.toString),
      customerProfile = customerProfile,
      templateData = triggered.templateData,
      expireAt = triggered.expireAt,
      address = CustomerAddress(contactInfo.line1,
                                Some(contactInfo.line2),
                                contactInfo.town,
                                Some(contactInfo.county),
                                contactInfo.postcode,
                                Some(contactInfo.country))
    )

    sendEvent(orchestratedPrintEvent)
  }
}
