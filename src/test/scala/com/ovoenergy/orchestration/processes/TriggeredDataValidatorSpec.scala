package com.ovoenergy.orchestration.processes

import java.time.Instant
import java.util.UUID

import com.ovoenergy.comms.model.{OrchestrationError, TriggeredV4}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalatest.{Matchers, WordSpec}
import monocle.macros.syntax.lens._
import org.scalacheck.Arbitrary
import org.scalacheck.Shapeless._

import scala.util.Random

class TriggeredDataValidatorSpec extends WordSpec with Matchers with ArbGenerator {

  implicit def arbUUID: Arbitrary[UUID] = Arbitrary {
    UUID.randomUUID()
  }
  implicit def arbInstant: Arbitrary[Instant] = Arbitrary {
    Instant.now().plusSeconds(Random.nextInt(5))
  }

  // Ensure we don't get empty strings
  implicit def arbString: Arbitrary[String] = Arbitrary {
    UUID.randomUUID().toString
  }

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
