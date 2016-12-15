package com.ovoenergy.orchestration.processes.email

import akka.Done
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.profile.CustomerProfiler.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import com.ovoenergy.orchestration.util.TestUtil
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future

class EmailOrchestrationSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with OneInstancePerTest {

  implicit val config = PatienceConfig()

  var producerInvocationCount = 0
  var passedOrchestratedEmail: OrchestratedEmail = _
  def producer = (orchestratedEmail: OrchestratedEmail) => {
    producerInvocationCount = producerInvocationCount + 1
    passedOrchestratedEmail = orchestratedEmail
    Future.successful(Done)
  }

  behavior of "EmailOrchestration"

  it should "fail when required parameters not set" in {
    val badCustomerProfile = CustomerProfile(
      CustomerProfileName("Mr", "", "", "Esq"),
      CustomerProfileEmailAddresses("", "")
    )

    val future = EmailOrchestration(producer)(badCustomerProfile, TestUtil.triggered)
    whenReady(future.failed) { error =>
      error.getMessage should include("Customer has no email address")
      error.getMessage should include("Customer has no last name")
      error.getMessage should include("Customer has no first name")
      producerInvocationCount shouldBe 0
    }
  }

  it should "call producer with primary email address if exists" in {

    val customerProfile = CustomerProfile(
      CustomerProfileName("Mr", "John", "Smith", ""),
      CustomerProfileEmailAddresses("some.email@ovoenergy.com", "some.other.email@ovoenergy.com")
    )

    val future = EmailOrchestration(producer)(customerProfile, TestUtil.triggered)
    whenReady(future) { result =>
      producerInvocationCount shouldBe 1
      passedOrchestratedEmail.recipientEmailAddress shouldBe "some.email@ovoenergy.com"
      passedOrchestratedEmail.customerProfile shouldBe model.CustomerProfile("John", "Smith")
      passedOrchestratedEmail.templateData shouldBe TestUtil.templateData
      passedOrchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
    }
  }

  it should "call producer with secondary email address if no primary exists" in {

    val customerProfile = CustomerProfile(
      CustomerProfileName("Mr", "John", "Smith", ""),
      CustomerProfileEmailAddresses("", "some.other.email@ovoenergy.com")
    )

    val future = EmailOrchestration(producer)(customerProfile, TestUtil.triggered)
    whenReady(future) { result =>
      producerInvocationCount shouldBe 1
      passedOrchestratedEmail.recipientEmailAddress shouldBe "some.other.email@ovoenergy.com"
      passedOrchestratedEmail.customerProfile shouldBe model.CustomerProfile("John", "Smith")
      passedOrchestratedEmail.templateData shouldBe TestUtil.templateData
      passedOrchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
    }
  }

}
