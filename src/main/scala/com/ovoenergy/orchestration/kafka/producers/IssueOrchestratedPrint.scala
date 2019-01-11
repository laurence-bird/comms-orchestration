package com.ovoenergy.orchestration.kafka.producers

import java.util.UUID

import cats.effect.{Async, IO}
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.print.OrchestratedPrintV2
import com.ovoenergy.orchestration.domain.ContactAddress
import org.apache.kafka.clients.producer.RecordMetadata

class IssueOrchestratedPrint(topic: Topic[OrchestratedPrintV2])
    extends IssueOrchestratedComm[ContactAddress] {

  val produceOrchestratedPrint =
    Producer.publisherFor[OrchestratedPrintV2](topic, _.metadata.commId)

  override def send(customerProfile: Option[CustomerProfile],
                    contactInfo: ContactAddress,
                    triggered: TriggeredV4): IO[RecordMetadata] = {

    val orchestratedPrintEvent = OrchestratedPrintV2(
      metadata = MetadataV3.fromSourceMetadata(
        "orchestration",
        triggered.metadata,
        triggered.metadata.commId ++ "-orchestrated"
      ),
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

    produceOrchestratedPrint(orchestratedPrintEvent)
  }
}
