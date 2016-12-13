package com.ovoenergy.orchestration.processes.email

import java.util.UUID

import akka.Done
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.profile.CustomerProfiler.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future

class EmailOrchestrationSpec extends FlatSpec
  with Matchers
  with ScalaFutures {

  implicit val config = PatienceConfig()

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

  behavior of "EmailOrchestration"

  it should "fail when required parameters not set" in {
    val successProducer = (orchestratedEmail: OrchestratedEmail) => {
      Future.successful(Done)
    }
    val badCustomerProfile = CustomerProfile(
      CustomerProfileName("Mr", "", "", "Esq"),
      CustomerProfileEmailAddresses("", "")
    )

    val future = EmailOrchestration(successProducer)(badCustomerProfile, triggered)
    whenReady(future.failed) { error =>
      error.getMessage should include("Customer has no email address")
      error.getMessage should include("Customer has no last name")
      error.getMessage should include("Customer has no first name")
    }

  }

}
