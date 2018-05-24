package com.ovoenergy.orchestration.kafka.consumers

import java.util.UUID

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash

object EventConverter {

  implicit class TriggeredV3ToV4(v3: TriggeredV3) {
    val toV4: TriggeredV4 =
      TriggeredV4(
        metadata          = v3.metadata.toV3,
        templateData      = v3.templateData,
        deliverAt         = v3.deliverAt,
        expireAt          = v3.expireAt,
        preferredChannels = v3.preferredChannels
      )
  }

  implicit class CancelledRequestV2ToV3(v2: CancellationRequestedV2) {
    val toV3: CancellationRequestedV3 =
      CancellationRequestedV3(
        metadata    = v2.metadata.toV3,
        templateId  = Hash(v2.commName),
        customerId  = v2.customerId
      )
  }

  implicit class MetadataV2ToV3(v2: MetadataV2) {
    val toV3: MetadataV3 =
      MetadataV3(
        createdAt           = v2.createdAt,
        eventId             = v2.eventId,
        traceToken          = v2.traceToken,
        commId              = UUID.randomUUID().toString,
        deliverTo           = v2.deliverTo,
        templateManifest    = TemplateManifest(Hash(v2.commManifest.name), v2.commManifest.version),
        friendlyDescription = v2.friendlyDescription,
        source              = v2.source,
        canary              = v2.canary,
        sourceMetadata      = v2.sourceMetadata.map(_.toV3),
        triggerSource       = v2.triggerSource
      )
  }

  implicit class GenericMetadataV2ToV3(v2: GenericMetadataV2) {
    val toV3: GenericMetadataV3 =
      GenericMetadataV3(
        createdAt   = v2.createdAt,
        eventId     = v2.eventId,
        traceToken  = v2.traceToken,
        commId      = UUID.randomUUID().toString,
        source      = v2.source,
        canary      = v2.canary
      )
  }

}