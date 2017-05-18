package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2

trait HasCommName[A] {
  def commName(a: A): String
}

object HasCommName {
  def instance[A](getCommName: A => String): HasCommName[A] = new HasCommName[A] {
    def commName(a: A): String = getCommName(a)
  }

  implicit val failedCancellationHasCommName = HasCommName.instance[FailedCancellationV2](_.cancellationRequested.commName)
  implicit val failedHasCommName      = HasCommName.instance[FailedV2](_.metadata.commManifest.name)
  implicit val orchStartedHasCommName = HasCommName.instance[OrchestrationStartedV2](_.metadata.commManifest.name)
  implicit val cancelledHasCommName   = HasCommName.instance[CancelledV2](_.cancellationRequested.commName)
  implicit val OrchestratedEmailHasCommName = HasCommName.instance[OrchestratedEmailV3](_.metadata.commManifest.name)
  implicit val OrchestratedSMSHasCommName = HasCommName.instance[OrchestratedSMSV2](_.metadata.commManifest.name)
}
