package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.{OrchestrationError, TemplateData, TriggeredV4}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbInstances
import org.scalatest.{Matchers, WordSpec}
import org.scalacheck.Shapeless._
import monocle.macros.syntax.lens._

class TriggeredDataValidatorSpec extends WordSpec with Matchers with ArbInstances {

  val triggeredV4 = generate[TriggeredV4]

  "EventDataValidator" should {

    "return the same triggeredV4 event" in {
      TriggeredDataValidator(triggeredV4) shouldBe Right(triggeredV4)
    }

    "reject triggeredV4 event" when {

      "templateId is empty" in {
        val empty = triggeredV4.lens(_.metadata.templateManifest.id).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: templateId", OrchestrationError))
      }

      "templateVersion is empty" in {
        val empty = triggeredV4.lens(_.metadata.templateManifest.version).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: templateVersion", OrchestrationError))
      }

      "commId is empty" in {
        val empty = triggeredV4.lens(_.metadata.commId).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: commId", OrchestrationError))
      }
    }
  }
}
