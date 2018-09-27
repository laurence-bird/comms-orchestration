package com.ovoenergy.orchestration.kafka.consumers

import java.util.UUID

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash

object EventConverter {

  implicit class CancelledRequestV2ToV3(cancellationRequestedV2: CancellationRequestedV2) {
    def toV3: CancellationRequestedV3 =
      CancellationRequestedV3(
        metadata = cancellationRequestedV2.metadata.toV3,
        templateId = Hash(cancellationRequestedV2.commName),
        customerId = cancellationRequestedV2.customerId
      )
  }

  implicit class GenericMetadataV2ToV3(v2GenericMetadata: GenericMetadataV2) {
    def toV3: GenericMetadataV3 =
      GenericMetadataV3(
        createdAt = v2GenericMetadata.createdAt,
        eventId = v2GenericMetadata.eventId,
        traceToken = v2GenericMetadata.traceToken,
        commId = UUID.randomUUID().toString,
        source = v2GenericMetadata.source,
        canary = v2GenericMetadata.canary
      )
  }

}
