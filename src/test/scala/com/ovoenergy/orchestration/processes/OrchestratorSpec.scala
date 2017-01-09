package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.{OrchestrationError, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorStuff
import com.ovoenergy.orchestration.util.TestUtil
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future

class OrchestratorSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with OneInstancePerTest
  with EitherValues {

  var passedCustomerProfile: CustomerProfile = _
  var passedTriggered: Triggered = _
  var invocationCount: Int = 0
  def emailOrchestrator = (customerProfile: CustomerProfile, triggered: Triggered) => {
    passedCustomerProfile = customerProfile
    passedTriggered = triggered
    invocationCount = invocationCount + 1
    Right(Future.successful(Done))
  }

  val customerProfile = CustomerProfile(CustomerProfileName(Some("Mr"), "John", "Smith", None), CustomerProfileEmailAddresses(Some("some.email@ovoenergy.com"), None))
  def customerProfiler = (customerId: String, canary: Boolean, traceToken: String) => {
    Right(customerProfile)
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile) => Right(SMS)
    val orchestrator = Orchestrator(customerProfiler, selectNonSupportedChannel, emailOrchestrator)(TestUtil.triggered)

    orchestrator.left.value shouldBe ErrorStuff("Unsupported channel selected SMS", OrchestrationError)
  }

  it should "handle failed channel selection" in {
    def failedChannelSelection = (customerProfile: CustomerProfile) => Left(ErrorStuff("whatever", OrchestrationError))
    val orchestrator = Orchestrator(customerProfiler, failedChannelSelection, emailOrchestrator)(TestUtil.triggered)
    orchestrator.left.value shouldBe ErrorStuff("whatever", OrchestrationError)
  }

  it should "handle failed customer profiler" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    def badCustomerProfiler: (String, Boolean, String) => Either[ErrorStuff, CustomerProfile] = (customerId: String, canary: Boolean, traceToken: String) => Left(ErrorStuff("whatever", ProfileRetrievalFailed))
    val orchestrator = Orchestrator(badCustomerProfiler, selectEmailChannel, emailOrchestrator)(TestUtil.triggered)
    orchestrator.left.value shouldBe ErrorStuff("whatever", ProfileRetrievalFailed)
  }

  it should "handle email channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    val orchestrator = Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator)(TestUtil.triggered)
    whenReady(orchestrator.right.value) { result =>
      invocationCount shouldBe 1
      passedCustomerProfile shouldBe customerProfile
      passedTriggered shouldBe TestUtil.triggered
    }
  }

}
