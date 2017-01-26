package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.{Failed, OrchestratedEmail, Triggered}
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Decoders._
import io.circe.generic.auto._

object Serialisation {

  val orchestratedEmailSerializer = avroSerializer[OrchestratedEmail]
  val orchestratedEmailDeserializer = avroDeserializer[OrchestratedEmail]
  val triggeredSerializer = avroSerializer[Triggered]
  val triggeredDeserializer = avroDeserializer[Triggered]
  val failedSerializer = avroSerializer[Failed]
  val failedDeserializer = avroDeserializer[Failed]

}
