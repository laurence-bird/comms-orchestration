package com.ovoenergy.orchestration.util

import java.util.UUID

import com.ovoenergy.comms.model._
import shapeless.Coproduct

object TestUtil {

  val traceToken          = "fpwfj2i0jr02jr2j0"
  val createdAt           = "2019-01-01T12:34:44.222Z"
  val customerId          = "GT-CUS-994332344"
  val friendlyDescription = "The customer did something cool and wants to know"
  val commManifest        = CommManifest(CommType.Service, "Plain old email", "1.0")
  val templateDataV1      = Map("someKey" -> "someValue")
  val templateData        = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue")))

  val metadata = Metadata(
    createdAt = createdAt,
    eventId = UUID.randomUUID().toString,
    customerId = customerId,
    traceToken = traceToken,
    friendlyDescription = friendlyDescription,
    source = "tests",
    sourceMetadata = None,
    commManifest = commManifest,
    canary = false,
    triggerSource = "test-trigger"
  )

  val triggeredV1 = Triggered(
    metadata = metadata,
    templateData = templateDataV1
  )

  val triggered = TriggeredV2(
    metadata = metadata,
    templateData = templateData,
    deliverAt = None,
    expireAt = None
  )
}
