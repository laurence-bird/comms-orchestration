package com.ovoenergy.orchestration.util

import java.util.UUID

import com.ovoenergy.comms.model.{CommManifest, CommType, Metadata, Triggered}

object TestUtil {

  val traceToken = "fpwfj2i0jr02jr2j0"
  val createdAt = "2019-01-01T12:34:44.222Z"
  val customerId = "GT-CUS-994332344"
  val friendlyDescription = "The customer did something cool and wants to know"
  val commManifest = CommManifest(CommType.Service, "Plain old email", "1.0")
  val templateData = Map("someKey" -> "someValue")

  val metadata = Metadata(
    createdAt = createdAt,
    eventId = UUID.randomUUID().toString,
    customerId = customerId,
    traceToken = traceToken,
    friendlyDescription = friendlyDescription,
    source = "tests",
    sourceMetadata = None,
    commManifest = commManifest,
    canary = false)

  val triggered = Triggered(
    metadata = metadata,
    data = templateData
  )

}
