package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import com.ovoenergy.orchestration.util.TestUtil
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class OrchestratorSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with OneInstancePerTest {

  var passedCustomerProfile: CustomerProfile = _
  var passedTriggered: Triggered = _
  var invocationCount: Int = 0
  def emailOrchestrator = (customerProfile: CustomerProfile, triggered: Triggered) => {
    passedCustomerProfile = customerProfile
    passedTriggered = triggered
    invocationCount = invocationCount + 1
    Future.successful(Done)
  }

  val customerProfile = CustomerProfile(CustomerProfileName(Some("Mr"), "John", "Smith", None), CustomerProfileEmailAddresses(Some("some.email@ovoenergy.com"), None))
  def customerProfiler = (customerId: String) => {
    Success(customerProfile)
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile) => Success(SMS)
    val future = Orchestrator(customerProfiler, selectNonSupportedChannel, emailOrchestrator)(TestUtil.triggered)
    whenReady(future.failed) { result =>
      invocationCount shouldBe 0
    }
  }

  it should "handle failed channel selection" in {
    def failedChannelSelection = (customerProfile: CustomerProfile) => Failure(new Exception("whatever"))
    val future = Orchestrator(customerProfiler, failedChannelSelection, emailOrchestrator)(TestUtil.triggered)
    whenReady(future.failed) { result =>
      invocationCount shouldBe 0
    }
  }

  it should "handle failed customer profiler" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Success(Email)
    def badCustomerProfiler = (customerId: String) => Failure(new Exception("whatever"))
    val future = Orchestrator(badCustomerProfiler, selectEmailChannel, emailOrchestrator)(TestUtil.triggered)
    whenReady(future.failed) { result =>
      invocationCount shouldBe 0
    }
  }

  it should "handle email channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Success(Email)
    val future = Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator)(TestUtil.triggered)
    whenReady(future) { result =>
      invocationCount shouldBe 1
      passedCustomerProfile shouldBe customerProfile
      passedTriggered shouldBe TestUtil.triggered
    }
  }



}
