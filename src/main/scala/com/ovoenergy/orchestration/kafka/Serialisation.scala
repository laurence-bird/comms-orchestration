package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.{OrchestratedEmailV2, OrchestratedEmailV3}
import com.ovoenergy.comms.model.sms.{OrchestratedSMS, OrchestratedSMSV2}
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Codecs._

object Serialisation {

  val orchestratedEmailSerializer             = avroSerializer[OrchestratedEmailV3]
  val orchestratedSMSSerializer               = avroSerializer[OrchestratedSMSV2]
  val legacyTriggeredDeserializer             = hackyAvroDeserializerForTriggeredV2[TriggeredV2]
  val triggeredDeserializer                   = avroDeserializer[TriggeredV3]
  val failedV2Serializer                      = avroSerializer[FailedV2]
  val cancelledSerializer                     = avroSerializer[CancelledV2]
  val orchestrationStartedV2Serializer        = avroSerializer[OrchestrationStartedV2]
  val legacyCancellationRequestedDeserializer = avroDeserializer[CancellationRequested]
  val cancellationRequestedDeserializer       = avroDeserializer[CancellationRequestedV2]
  val failedCancellationSerializer            = avroSerializer[FailedCancellationV2]

}
