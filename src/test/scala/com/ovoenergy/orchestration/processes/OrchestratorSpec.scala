package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{ContactProfile, CustomerProfile}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.{ArbGenerator, TestUtil}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Record
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import org.scalacheck.Shapeless._

import scala.concurrent.Future

class OrchestratorSpec
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with OneInstancePerTest
    with EitherValues
    with ArbGenerator
    with BeforeAndAfterEach {

  var orchestratedDeets: Option[(Option[model.CustomerProfile], String)] = None
  var passedTriggered: Option[TriggeredV3]                               = None

  val customerProfile   = generate[CustomerProfile]
  val contactProfile    = generate[ContactProfile]
  val customerTriggered = TestUtil.customerTriggered
  val internalMetadata  = generate[InternalMetadata]

  val emailOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.email", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)
  val SMSOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.SMS", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)

  def emailOrchestrator =
    (customerProfile: Option[model.CustomerProfile], deliverTo: String, triggered: TriggeredV3,
     internalMetadata: InternalMetadata) => {
      orchestratedDeets = Some(customerProfile, deliverTo)
      passedTriggered = Some(triggered)
      Future.successful(emailOrchestratedMetadata)
    }

  def smsOrchestrator =
    (customerProfile: Option[model.CustomerProfile], deliverTo: String, triggered: TriggeredV3,
     internalMetadata: InternalMetadata) => {
      orchestratedDeets = Some(customerProfile, deliverTo)
      passedTriggered = Some(triggered)
      Future.successful(SMSOrchestratedMetadata)
    }

  private def customerProfiler(profile: CustomerProfile) =
    (customerId: String, canary: Boolean, traceToken: String) => {
      Right(profile)
    }

  def nonCustomerProfiler(customerId: String, canary: Boolean, traceToken: String) = {
    fail("Customer profile shouldn't be invoked")
  }

  val validProfile = (customerProfile: CustomerProfile) => Right(customerProfile)
  val invalidProfile = (customerProfile: CustomerProfile) =>
    Left(ErrorDetails("Name missing from profile", InvalidProfile))

  override def beforeEach(): Unit = {
    orchestratedDeets = None
    passedTriggered = None
  }

  behavior of "Orchestrator"

  it should "handle customer delivery - unsupported channels" in {
    def selectNonSupportedChannel = (contactProfile: ContactProfile, triggered: TriggeredV3) => Right(Post)
    val orchestrator = //(CustomerProfile, TriggeredV2, InternalMetadata)
    Orchestrator(customerProfiler(customerProfile),
                 selectNonSupportedChannel,
                 validProfile,
                 emailOrchestrator,
                 smsOrchestrator)(customerTriggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("Unsupported channel selected Post", OrchestrationError)
  }

  it should "handle customer delivery - failed channel selection" in {
    def failedChannelSelection =
      (contactProfile: ContactProfile, triggered: TriggeredV3) => Left(ErrorDetails("whatever", OrchestrationError))
    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(customerProfiler(customerProfile),
                   failedChannelSelection,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(customerTriggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle customer delivery - failed customer profiler" in {
    def selectEmailChannel = (contactProfile: ContactProfile, triggered: TriggeredV3) => Right(Email)
    def badCustomerProfiler: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile] =
      (customerId: String, canary: Boolean, traceToken: String) =>
        Left(ErrorDetails("whatever", ProfileRetrievalFailed))
    val orchestrator =
      Orchestrator(badCustomerProfiler, selectEmailChannel, validProfile, emailOrchestrator, smsOrchestrator)(
        customerTriggered,
        internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle customer delivery - email channel" in {
    val profileWithEmailAddress =
      customerProfile.copy(contactProfile = contactProfile.copy(emailAddress = Some("mrtest@testing.com")))
    def selectEmailChannel = (contactProfile: ContactProfile, triggered: TriggeredV3) => Right(Email)
    val orchestrationResult =
      Orchestrator(customerProfiler(profileWithEmailAddress),
                   selectEmailChannel,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(customerTriggered, internalMetadata)
    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe emailOrchestratedMetadata
      orchestratedDeets shouldBe Some(
        Some(model.CustomerProfile(profileWithEmailAddress.name.firstName, profileWithEmailAddress.name.lastName)),
        profileWithEmailAddress.contactProfile.emailAddress.get
      )
      passedTriggered shouldBe Some(customerTriggered)
    }
  }

  it should "handle customer delivery - SMS channel" in {
    val profileWithPhoneNumber =
      customerProfile.copy(contactProfile = contactProfile.copy(phoneNumber = Some("12345678")))
    def selectEmailChannel = (contactProfile: ContactProfile, triggered: TriggeredV3) => Right(SMS)
    val orchestrator =
      Orchestrator(customerProfiler(profileWithPhoneNumber),
                   selectEmailChannel,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(customerTriggered, internalMetadata)
    whenReady(orchestrator.right.value) { resultMetadata =>
      resultMetadata shouldBe SMSOrchestratedMetadata
      orchestratedDeets shouldBe Some(
        Some(model.CustomerProfile(profileWithPhoneNumber.name.firstName, profileWithPhoneNumber.name.lastName)),
        profileWithPhoneNumber.contactProfile.phoneNumber.get
      )
      passedTriggered shouldBe Some(customerTriggered)
    }
  }

  it should "handle non customer delivery - email channel" in {
    val contactDetails = ContactDetails(Some("email@address.com"), None)
    val nonCustomerTriggered =
      generate[TriggeredV3].copy(metadata = generate[MetadataV2].copy(deliverTo = contactDetails))

    def selectEmailChannel = (contactProfile: ContactProfile, triggered: TriggeredV3) => Right(Email)
    val orchestrationResult =
      Orchestrator(nonCustomerProfiler, selectEmailChannel, validProfile, emailOrchestrator, smsOrchestrator)(
        nonCustomerTriggered,
        internalMetadata)
    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe emailOrchestratedMetadata
      orchestratedDeets shouldBe Some(None, contactDetails.emailAddress.get)
      passedTriggered shouldBe Some(nonCustomerTriggered)
    }
  }
}
