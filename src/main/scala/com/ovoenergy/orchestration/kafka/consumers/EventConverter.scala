package com.ovoenergy.comms.orchestration.kafka.consumers

import java.util.UUID

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash

object EventConverter {

  implicit class TriggeredV3ToV4(triggeredV3: TriggeredV3) {
    def toV4: TriggeredV4 =
      TriggeredV4(
        metadata = triggeredV3.metadata.toV3Metadata,
        templateData = triggeredV3.templateData,
        deliverAt = triggeredV3.deliverAt,
        expireAt = triggeredV3.expireAt,
        preferredChannels = triggeredV3.preferredChannels
      )
  }

  implicit class CancelledRequestV2ToV3(cancellationRequestedV2: CancellationRequestedV2) {
    def toV3: CancellationRequestedV3 =
      CancellationRequestedV3(
        metadata = cancellationRequestedV2.metadata.toV3,
        templateId = Hash(cancellationRequestedV2.commName),
        customerId = cancellationRequestedV2.customerId
      )
  }

  implicit class MetadataV2ToV3(v2Metadata: MetadataV2) {
    def toV3Metadata: MetadataV3 =
      MetadataV3(
        createdAt = v2Metadata.createdAt,
        eventId = v2Metadata.eventId,
        traceToken = v2Metadata.traceToken,
        commId = UUID.randomUUID().toString,
        deliverTo = v2Metadata.deliverTo,
        templateManifest = TemplateManifest(Hash(v2Metadata.commManifest.name), v2Metadata.commManifest.version),
        friendlyDescription = v2Metadata.friendlyDescription,
        source = v2Metadata.source,
        canary = v2Metadata.canary,
        sourceMetadata = v2Metadata.sourceMetadata.map(_.toV3Metadata),
        triggerSource = v2Metadata.triggerSource
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
