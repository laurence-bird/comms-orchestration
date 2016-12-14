package com.ovoenergy.orchestration.processes

import java.util.UUID

import akka.Done
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.profile.CustomerProfiler.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class OrchestratorSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with OneInstancePerTest {

  val traceToken = "fpwfj2i0jr02jr2j0"
  val createdAt = "2019-01-01T12:34:44.222Z"
  val customerId = "GT-CUS-994332344"
  val friendlyDescription = "The customer did something cool and wants to know"
  val commManifest = CommManifest(CommType.Service, "Plain old email", "1.0")

  val metadata = Metadata(
    createdAt = createdAt,
    eventId = UUID.randomUUID().toString,
    customerId = customerId,
    traceToken = traceToken,
    friendlyDescription = friendlyDescription,
    source = "tests",
    sourceMetadata = None,
    commManifest = commManifest,
    canary = false)

  val triggered = Triggered(
    metadata = metadata,
    data = Map()
  )

  var passedCustomerProfile: CustomerProfile = _
  var passedTriggered: Triggered = _
  var invocationCount: Int = 0
  def emailOrchestrator = (customerProfile: CustomerProfile, triggered: Triggered) => {
    passedCustomerProfile = customerProfile
    passedTriggered = triggered
    invocationCount = invocationCount + 1
    Future.successful(Done)
  }

  val customerProfile = CustomerProfile(CustomerProfileName("Mr", "John", "Smith", ""), CustomerProfileEmailAddresses("some.email@ovoenergy.com", ""))
  def customerProfiler = (customerId: String) => {
    customerProfile
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile) => SMS
    val future = Orchestrator(customerProfiler, selectNonSupportedChannel, emailOrchestrator)(triggered)
    whenReady(future) { result =>
      //OK
    }
    invocationCount shouldBe 0
  }

  it should "handle email channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Email
    val future = Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator)(triggered)
    whenReady(future) { result =>
      //OK
    }
    invocationCount shouldBe 1
    passedCustomerProfile shouldBe customerProfile
    passedTriggered shouldBe triggered
  }



}
