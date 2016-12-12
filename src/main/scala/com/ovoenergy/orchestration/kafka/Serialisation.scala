package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.{ComposedEmail, Triggered}
import com.ovoenergy.comms.serialisation.Serialisation._

object Serialisation {

  val composedEmailSerializer = avroSerializer[ComposedEmail]
  val composedEmailDeserializer = avroDeserializer[ComposedEmail]
  val triggeredSerializer = avroSerializer[Triggered]
  val triggeredDeserializer = avroDeserializer[Triggered]

}
