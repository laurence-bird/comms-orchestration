package com.ovoenergy.orchestration.util

import java.time.Instant
import java.util.UUID
import com.ovoenergy.comms.model._
import shapeless.Coproduct

object TestUtil {

  val traceToken          = "fpwfj2i0jr02jr2j0"
  val createdAt           = "2019-01-01T12:34:44.222Z"
  val customerId          = "GT-CUS-994332344"
  val friendlyDescription = "The customer did something cool and wants to know"
  val commManifest        = CommManifest(Service, "test-comm", "1.0")
  val templateDataV1      = Map("someKey" -> "someValue")
  val templateData        = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue")))

  val metadataV2 = MetadataV2(
    createdAt = Instant.parse(createdAt),
    eventId = UUID.randomUUID().toString,
    deliverTo = Customer(customerId),
    traceToken = traceToken,
    friendlyDescription = friendlyDescription,
    source = "tests",
    sourceMetadata = None,
    commManifest = commManifest,
    canary = false,
    triggerSource = "test-trigger"
  )

  val customerTriggered = TriggeredV3(
    metadata = metadataV2,
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  val emailContactDetailsTriggered = TriggeredV3(
    metadata = metadataV2.copy(deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None)),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  val smsContactDetailsTriggered = TriggeredV3(
    metadata = metadataV2.copy(deliverTo = ContactDetails(None, Some("+447985631544"))),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  val invalidContactDetailsTriggered = TriggeredV3(
    metadata = metadataV2.copy(deliverTo = ContactDetails(None, None)),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )
}
