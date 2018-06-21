package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.{OrchestrationError, TemplateData, TriggeredV3, TriggeredV4}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalatest.{Matchers, WordSpec}
import org.scalacheck.Shapeless._
import monocle.macros.syntax.lens._

class TriggeredDataValidatorSpec extends WordSpec with Matchers with ArbGenerator {

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

  val triggeredV3 = generate[TriggeredV3]

  "EventDataValidator" should {

    "return the same triggeredV3 event" in {
      TriggeredDataValidator(triggeredV3) shouldBe Right(triggeredV3)
    }

    "reject triggeredV3 event" when {

      "traceToken is empty" in {
        val empty = triggeredV3.lens(_.metadata.traceToken).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: traceToken", OrchestrationError))
      }

      "eventId is empty" in {
        val empty = triggeredV3.lens(_.metadata.eventId).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: eventId", OrchestrationError))
      }

      "friendlyDescription is empty" in {
        val empty = triggeredV3.lens(_.metadata.friendlyDescription).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: friendlyDescription", OrchestrationError))
      }

      "source is empty" in {
        val empty = triggeredV3.lens(_.metadata.source).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: source", OrchestrationError))
      }

      "triggerSource is empty" in {
        val empty = triggeredV3.lens(_.metadata.triggerSource).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: triggerSource", OrchestrationError))
      }

      "commManifestName is empty" in {
        val empty = triggeredV3.lens(_.metadata.commManifest.name).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: commManifestName", OrchestrationError))
      }

      "commManifestVersion is empty" in {
        val empty = triggeredV3.lens(_.metadata.commManifest.version).modify(_ => "")
        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails("The following fields contain empty string: commManifestVersion", OrchestrationError))
      }

      "multiple strings are empty" in {
        val empty = triggeredV3
          .lens(_.metadata.traceToken)
          .modify(_ => "")
          .lens(_.metadata.eventId)
          .modify(_ => "")
          .lens(_.metadata.friendlyDescription)
          .modify(_ => "")
          .lens(_.metadata.source)
          .modify(_ => "")
          .lens(_.metadata.triggerSource)
          .modify(_ => "")
          .lens(_.metadata.commManifest.name)
          .modify(_ => "")
          .lens(_.metadata.commManifest.version)
          .modify(_ => "")

        TriggeredDataValidator(empty) shouldBe Left(ErrorDetails(
          "The following fields contain empty string: traceToken, eventId, friendlyDescription, source, triggerSource, commManifestName, commManifestVersion",
          OrchestrationError
        ))
      }

      "template data contains empty field" in {

        val td = TemplateData.fromMap(
          Map(
            "firstName" -> TemplateData.fromString("Joe"),
            "lastName"  -> TemplateData.fromString(""),
            "roles" -> TemplateData.fromSeq(
              List[TemplateData](TemplateData.fromString("admin"), TemplateData.fromString("")))
          ))

        val empty = triggeredV3.lens(_.templateData).modify(_ => Map("person" -> td))

        TriggeredDataValidator(empty) shouldBe Left(
          ErrorDetails(
            "The following fields contain empty string: templateData.person.lastName, templateData.person.roles",
            OrchestrationError))

      }
    }
  }
}
