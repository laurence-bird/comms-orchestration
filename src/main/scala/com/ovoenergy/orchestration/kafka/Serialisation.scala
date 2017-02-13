package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Decoders._
import io.circe.generic.auto._

object Serialisation {

  val orchestratedEmailV2Serializer = avroSerializer[OrchestratedEmailV2]
  val triggeredV2Deserializer = avroDeserializer[TriggeredV2]
  val failedSerializer = avroSerializer[Failed]

}
