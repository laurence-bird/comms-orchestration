package com.ovoenergy.orchestration.util

import java.time.Instant
import java.util.UUID
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import shapeless.Coproduct

object TestUtil {

  val traceToken          = "fpwfj2i0jr02jr2j0"
  val createdAt           = "2019-01-01T12:34:44.222Z"
  val customerId          = "GT-CUS-994332344"
  val commName            = "test-comm"
  val friendlyDescription = "The customer did something cool and wants to know"
  val commManifest        = CommManifest(Service, commName, "1.0")
  val templateManifest    = TemplateManifest(Hash("test-comm"), "1.0")
  def templateData        = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD](UUID.randomUUID().toString)))

  def metadataV3 = MetadataV3(
    commId = UUID.randomUUID().toString,
    createdAt = Instant.now(),
    eventId = UUID.randomUUID().toString,
    deliverTo = Customer(customerId),
    traceToken = traceToken,
    friendlyDescription = friendlyDescription,
    source = "tests",
    sourceMetadata = None,
    templateManifest = templateManifest,
    canary = false,
    triggerSource = "test-trigger"
  )

  def customerTriggeredV4 = TriggeredV4(
    metadata = metadataV3,
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  def emailContactDetailsTriggered = TriggeredV4(
    metadata = metadataV3.copy(deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None)),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  def smsContactDetailsTriggered = TriggeredV4(
    metadata = metadataV3.copy(deliverTo = ContactDetails(None, Some("+447985631544"))),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  def invalidContactDetailsTriggered = TriggeredV4(
    metadata = metadataV3.copy(deliverTo = ContactDetails(None, None)),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )
}
