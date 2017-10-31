package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.{
  CommunicationPreference,
  ContactAddress,
  ContactProfile,
  CustomerProfile,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.kafka.IssueOrchestratedComm
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

  var orchestratedDetails: Option[(Option[model.CustomerProfile], String)] = None
  var passedTriggered: Option[TriggeredV3]                                 = None
  var contactDetailsPassedToChannelSelection                               = Option.empty[ContactProfile]

  val customerProfile   = generate[CustomerProfile]
  val contactProfile    = generate[ContactProfile]
  val customerTriggered = TestUtil.customerTriggered
  val internalMetadata  = generate[InternalMetadata]

  val emailOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.email", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)
  val SMSOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.SMS", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)

  object StubEmailOrchestrator extends IssueOrchestratedComm[EmailAddress] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: EmailAddress,
                      triggered: TriggeredV3) = {
      orchestratedDetails = Some(customerProfile, contactInfo.address)
      passedTriggered = Some(triggered)
      Future.successful(emailOrchestratedMetadata)
    }
  }

  object StubSmsOrchestrator extends IssueOrchestratedComm[MobilePhoneNumber] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: MobilePhoneNumber,
                      triggered: TriggeredV3) = {
      orchestratedDetails = Some(customerProfile, contactInfo.number)
      passedTriggered = Some(triggered)
      Future.successful(SMSOrchestratedMetadata)
    }
  }

  object StubPrintOrchestrator extends IssueOrchestratedComm[ContactAddress] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: ContactAddress,
                      triggered: TriggeredV3) = {
      orchestratedDetails = Some(customerProfile, contactInfo.toString)
      passedTriggered = Some(triggered)
      Future.successful(SMSOrchestratedMetadata)
    }
  }

  private def customerProfiler(profile: CustomerProfile) =
    (customerId: String, canary: Boolean, traceToken: String) => {
      Right(profile)
    }

  def nonCustomerProfiler(customerId: String, canary: Boolean, traceToken: String) = {
    fail("Customer profile shouldn't be invoked")
  }

  val validCustomerProfile = (triggeredV3: TriggeredV3, customer: Customer) => Right(customerProfile)
  val validContactProfile  = (contactProfile: ContactProfile) => Right(contactProfile)
  val invalidProfile = (triggeredV3: TriggeredV3, customer: Customer) =>
    Left(ErrorDetails("Name missing from profile", InvalidProfile))

  override def beforeEach(): Unit = {
    orchestratedDetails = None
    passedTriggered = None
  }

  behavior of "Orchestrator"

  it should "Specify no postal address available for triggers to be orchestrated via print, where a customer ID is provided" in {
    object SelectNonSupportedChannel extends ChannelSelector {
      override def determineChannel(contactProfile: ContactProfile,
                                    customerPreferences: Seq[CommunicationPreference],
                                    triggered: TriggeredV3) = Right(Print)
    }
    val orchestrator =
      Orchestrator(SelectNonSupportedChannel,
                   validCustomerProfile,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(customerTriggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("No valid postal address provided", InvalidProfile)
  }

  it should "handle customer delivery - failed channel selection" in {
    object FailedChannelSelection extends ChannelSelector {
      override def determineChannel(contactProfile: ContactProfile,
                                    customerPreferences: Seq[CommunicationPreference],
                                    triggered: TriggeredV3) = Left(ErrorDetails("whatever", OrchestrationError))
    }
    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(FailedChannelSelection,
                   validCustomerProfile,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(customerTriggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle customer delivery - failed customer profiler" in {
    val badCustomerProfiler = (triggeredV3: TriggeredV3, customer: Customer) =>
      Left(ErrorDetails("whatever", ProfileRetrievalFailed))

    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(SelectEmailChannel,
                   badCustomerProfiler,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(customerTriggered, internalMetadata)

    orchestrator.left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle customer delivery - email channel" in {
    val profileWithEmailAddress: CustomerProfile =
      customerProfile.copy(
        contactProfile = contactProfile.copy(emailAddress = Some(EmailAddress("mrtest@testing.com"))))

    val validCustomerProfileWithEmailAddress = (triggeredV3: TriggeredV3, customer: Customer) => Right(profileWithEmailAddress)
    val orchestrationResult =
      Orchestrator(SelectEmailChannel,
                   validCustomerProfileWithEmailAddress,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(customerTriggered, internalMetadata)

    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe emailOrchestratedMetadata
      orchestratedDetails shouldBe Some(
        Some(model.CustomerProfile(profileWithEmailAddress.name.firstName, profileWithEmailAddress.name.lastName)),
        profileWithEmailAddress.contactProfile.emailAddress.get.address
      )
      passedTriggered shouldBe Some(customerTriggered)
    }
  }

  it should "handle customer delivery - SMS channel" in {
    val profileWithPhoneNumber =
      customerProfile.copy(contactProfile = contactProfile.copy(mobileNumber = Some(MobilePhoneNumber("0799547896"))))

    val validCustomerProfileWithPhoneNumber = (triggeredV3: TriggeredV3, customer: Customer) => Right(profileWithPhoneNumber)
    val orchestrator =
      Orchestrator(SelectSMSChannel,
                   validCustomerProfileWithPhoneNumber,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(customerTriggered, internalMetadata)

    whenReady(orchestrator.right.value) { resultMetadata =>
      resultMetadata shouldBe SMSOrchestratedMetadata
      orchestratedDetails shouldBe Some(
        Some(model.CustomerProfile(profileWithPhoneNumber.name.firstName, profileWithPhoneNumber.name.lastName)),
        profileWithPhoneNumber.contactProfile.mobileNumber.get.number
      )
      contactDetailsPassedToChannelSelection shouldBe Some(
        ContactProfile(profileWithPhoneNumber.contactProfile.emailAddress,
                       (profileWithPhoneNumber.contactProfile.mobileNumber),
                       None))
      passedTriggered shouldBe Some(customerTriggered)
    }
  }

  it should "handle non customer delivery - email channel" in {
    val contactDetails = ContactDetails(Some("email@address.com"), None)

    val nonCustomerTriggered =
      generate[TriggeredV3].copy(metadata = generate[MetadataV2].copy(deliverTo = contactDetails))

    val orchestrationResult =
      Orchestrator(SelectEmailChannel,
                   validCustomerProfile,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(nonCustomerTriggered, internalMetadata)

    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe emailOrchestratedMetadata
      contactDetailsPassedToChannelSelection shouldBe Some(
        ContactProfile(Some(EmailAddress("email@address.com")), None, None))
      orchestratedDetails shouldBe Some(None, contactDetails.emailAddress.get)
      passedTriggered shouldBe Some(nonCustomerTriggered)
    }
  }

  it should "handle non customer delivery - sms channel" in {
    val contactDetails = ContactDetails(None, Some("07995447896"))

    val nonCustomerTriggered =
      generate[TriggeredV3].copy(metadata = generate[MetadataV2].copy(deliverTo = contactDetails))

    val orchestrationResult =
      Orchestrator(SelectSMSChannel,
                   validCustomerProfile,
                   validContactProfile,
                   StubEmailOrchestrator,
                   StubSmsOrchestrator,
                   StubPrintOrchestrator)(nonCustomerTriggered, internalMetadata)

    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe SMSOrchestratedMetadata
      contactDetailsPassedToChannelSelection shouldBe Some(
        ContactProfile(None, Some(MobilePhoneNumber("07995447896")), None))
      orchestratedDetails shouldBe Some(None, "07995447896")
      passedTriggered shouldBe Some(nonCustomerTriggered)
    }
  }

  object SelectSMSChannel extends ChannelSelector {
    override def determineChannel(contactProfile: ContactProfile,
                                  customerPreferences: Seq[CommunicationPreference],
                                  triggered: TriggeredV3) = {
      contactDetailsPassedToChannelSelection = Some(contactProfile)
      Right(SMS)
    }
  }

  object SelectEmailChannel extends ChannelSelector {
    override def determineChannel(contactProfile: ContactProfile,
                                  customerPreferences: Seq[CommunicationPreference],
                                  triggered: TriggeredV3) = {
      contactDetailsPassedToChannelSelection = Some(contactProfile)
      Right(Email)
    }
  }
}
