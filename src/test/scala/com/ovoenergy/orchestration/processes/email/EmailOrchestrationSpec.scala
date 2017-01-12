package com.ovoenergy.orchestration.processes.email

import akka.Done
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.ErrorCode.InvalidProfile
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalacheck.Shapeless._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future

class EmailOrchestrationSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with OneInstancePerTest
  with EitherValues
  with ArbGenerator {

  implicit val config = PatienceConfig()

  var producerInvocationCount = 0
  var passedOrchestratedEmail: OrchestratedEmail = _
  def producer = (orchestratedEmail: OrchestratedEmail) => {
    producerInvocationCount = producerInvocationCount + 1
    passedOrchestratedEmail = orchestratedEmail
    Future.successful(Done)
  }


  val customerProfile = generate[CustomerProfile]
  val triggered       = generate[Triggered]
  val internalMetadata = generate[InternalMetadata]

  behavior of "EmailOrchestration"

  it should "fail when required parameters not set" in {
    val badCustomerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "", "", Some("Esq")),
      CustomerProfileEmailAddresses(Some(""), None)
    )

    val error = EmailOrchestration(producer)(badCustomerProfile, triggered, internalMetadata).left.value

      error.reason should include("Customer has no usable email address")
      error.reason should include("Customer has no last name")
      error.reason should include("Customer has no first name")
      error.errorCode shouldBe InvalidProfile
  }

  it should "call producer with primary email address if exists" in {

    val customerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "John", "Smith", None),
      CustomerProfileEmailAddresses(Some("some.email@ovoenergy.com"), Some("some.other.email@ovoenergy.com"))
    )

    val future = EmailOrchestration(producer)(customerProfile, triggered, internalMetadata).right.value
    whenReady(future) { result =>
      producerInvocationCount shouldBe 1
      passedOrchestratedEmail.recipientEmailAddress shouldBe "some.email@ovoenergy.com"
      passedOrchestratedEmail.customerProfile shouldBe model.CustomerProfile("John", "Smith")
      passedOrchestratedEmail.templateData shouldBe triggered.templateData
      passedOrchestratedEmail.metadata.traceToken shouldBe triggered.metadata.traceToken
    }
  }

  it should "call producer with secondary email address if no primary exists" in {

    val customerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "John", "Smith", None),
      CustomerProfileEmailAddresses(None, Some("some.other.email@ovoenergy.com"))
    )

    val future = EmailOrchestration(producer)(customerProfile, triggered, internalMetadata).right.value
    whenReady(future) { result =>
      producerInvocationCount shouldBe 1
      passedOrchestratedEmail.recipientEmailAddress shouldBe "some.other.email@ovoenergy.com"
      passedOrchestratedEmail.customerProfile shouldBe model.CustomerProfile("John", "Smith")
      passedOrchestratedEmail.templateData shouldBe triggered.templateData
      passedOrchestratedEmail.metadata.traceToken shouldBe triggered.metadata.traceToken
    }
  }

}
