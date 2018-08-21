package com.ovoenergy.comms.orchestration.processes

import cats.effect.IO
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.domain.{
  CommunicationPreference,
  ContactAddress,
  ContactProfile,
  CustomerProfile,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.comms.orchestration.kafka.IssueOrchestratedComm
import com.ovoenergy.comms.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.comms.orchestration.util.{ArbGenerator, TestUtil}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import org.scalacheck.Shapeless._

class OrchestratorSpec
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with OneInstancePerTest
    with EitherValues
    with ArbGenerator
    with BeforeAndAfterEach {

  var orchestratedDetails: Option[(Option[model.CustomerProfile], String)] = None
  var passedTriggered: Option[TriggeredV4]                                 = None
  var contactDetailsPassedToChannelSelection                               = Option.empty[ContactProfile]

  val customerProfile   = generate[CustomerProfile]
  val contactProfile    = generate[ContactProfile]
  val customerTriggered = TestUtil.customerTriggeredV4
  val internalMetadata  = generate[InternalMetadata]

  implicit val ec = scala.concurrent.ExecutionContext.global

  val emailOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.email", 1), 1, 1, 100l, -1, -1, -1)
  val SMSOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.SMS", 1), 1, 1, 100l, -1, -1, -1)
  val printOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.print", 1), 1, 1, 100l, -1, -1, -1)

  object StubEmailOrchestrator extends IssueOrchestratedComm[EmailAddress, IO] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: EmailAddress,
                      triggered: TriggeredV4) = {
      orchestratedDetails = Some(customerProfile, contactInfo.address)
      passedTriggered = Some(triggered)
      IO.pure(emailOrchestratedMetadata)
    }
  }

  object StubSmsOrchestrator extends IssueOrchestratedComm[MobilePhoneNumber, IO] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: MobilePhoneNumber,
                      triggered: TriggeredV4) = {
      orchestratedDetails = Some(customerProfile, contactInfo.number)
      passedTriggered = Some(triggered)
      IO.pure(SMSOrchestratedMetadata)
    }
  }

  object StubPrintOrchestrator extends IssueOrchestratedComm[ContactAddress, IO] {
    override def send(customerProfile: Option[model.CustomerProfile],
                      contactInfo: ContactAddress,
                      triggered: TriggeredV4) = {
      orchestratedDetails = Some(customerProfile, contactInfo.toString)
      passedTriggered = Some(triggered)
      IO.pure(printOrchestratedMetadata)
    }
  }

  private def customerProfiler(profile: CustomerProfile) =
    (customerId: String, canary: Boolean, traceToken: String) => {
      Right(profile)
    }

  def nonCustomerProfiler(customerId: String, canary: Boolean, traceToken: String) = {
    fail("Customer profile shouldn't be invoked")
  }

  val validCustomerProfile = (triggeredV4: TriggeredV4, customer: Customer) => IO.pure(Right(customerProfile))
  val validContactProfile  = (contactProfile: ContactProfile) => Right(contactProfile)
  val invalidProfile = (triggeredV4: TriggeredV4, customer: Customer) =>
    Left(ErrorDetails("Name missing from profile", InvalidProfile))

  override def beforeEach(): Unit = {
    orchestratedDetails = None
    passedTriggered = None
  }

  behavior of "Orchestrator"

  it should "Specify no postal address available for triggers to be orchestrated via print, where a customer ID is provided" in {
    object SelectNonSupportedChannel extends ChannelSelector[IO] {
      override def determineChannel(contactProfile: ContactProfile,
                                    customerPreferences: Seq[CommunicationPreference],
                                    triggered: TriggeredV4) = IO.pure(Right(Print))
    }
    val orchestrator =
      Orchestrator[IO](SelectNonSupportedChannel,
                       validCustomerProfile,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(customerTriggered, internalMetadata)

    orchestrator.unsafeRunSync().left.value shouldBe ErrorDetails("No valid postal address provided", InvalidProfile)
  }

  it should "handle customer delivery - failed channel selection" in {
    object FailedChannelSelection extends ChannelSelector[IO] {
      override def determineChannel(contactProfile: ContactProfile,
                                    customerPreferences: Seq[CommunicationPreference],
                                    triggered: TriggeredV4) =
        IO.pure(Left(ErrorDetails("whatever", OrchestrationError)))
    }
    val orchestrator =
      Orchestrator[IO](FailedChannelSelection,
                       validCustomerProfile,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(customerTriggered, internalMetadata)
    orchestrator.unsafeRunSync().left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle customer delivery - failed customer profiler" in {
    val badCustomerProfiler =
      (triggeredV4: TriggeredV4, customer: Customer) => IO.pure(Left(ErrorDetails("whatever", ProfileRetrievalFailed)))

    val orchestrator =
      Orchestrator[IO](SelectEmailChannel,
                       badCustomerProfiler,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(customerTriggered, internalMetadata)

    orchestrator.unsafeRunSync().left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle customer delivery - email channel" in {
    val profileWithEmailAddress: CustomerProfile =
      customerProfile.copy(
        contactProfile = contactProfile.copy(emailAddress = Some(EmailAddress("mrtest@testing.com"))))

    val validCustomerProfileWithEmailAddress =
      (triggeredV4: TriggeredV4, customer: Customer) => IO.pure(Right(profileWithEmailAddress))
    val orchestrationResult =
      Orchestrator[IO](SelectEmailChannel,
                       validCustomerProfileWithEmailAddress,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(customerTriggered, internalMetadata)

    val resultMetadata = orchestrationResult.unsafeRunSync().right.value
    resultMetadata shouldBe emailOrchestratedMetadata
    orchestratedDetails shouldBe Some(
      Some(model.CustomerProfile(profileWithEmailAddress.name.firstName, profileWithEmailAddress.name.lastName)),
      profileWithEmailAddress.contactProfile.emailAddress.get.address
    )
    passedTriggered shouldBe Some(customerTriggered)
  }

  it should "handle customer delivery - SMS channel" in {
    val profileWithPhoneNumber =
      customerProfile.copy(contactProfile = contactProfile.copy(mobileNumber = Some(MobilePhoneNumber("0799547896"))))

    val validCustomerProfileWithPhoneNumber =
      (triggeredV4: TriggeredV4, customer: Customer) => IO.pure(Right(profileWithPhoneNumber))
    val orchestrationResult =
      Orchestrator[IO](SelectSMSChannel,
                       validCustomerProfileWithPhoneNumber,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(customerTriggered, internalMetadata)

    val resultMetadata = orchestrationResult.unsafeRunSync().right.value
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

  it should "handle non customer delivery - email channel" in {
    val contactDetails = ContactDetails(Some("email@address.com"), None)

    val nonCustomerTriggered =
      generate[TriggeredV4].copy(metadata = generate[MetadataV3].copy(deliverTo = contactDetails))

    val orchestrationResult =
      Orchestrator[IO](SelectEmailChannel,
                       validCustomerProfile,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(nonCustomerTriggered, internalMetadata)

    val resultMetadata = orchestrationResult.unsafeRunSync().right.value
    resultMetadata shouldBe emailOrchestratedMetadata
    contactDetailsPassedToChannelSelection shouldBe Some(
      ContactProfile(Some(EmailAddress("email@address.com")), None, None))
    orchestratedDetails shouldBe Some(None, contactDetails.emailAddress.get)
    passedTriggered shouldBe Some(nonCustomerTriggered)
  }

  it should "handle non customer delivery - sms channel" in {
    val contactDetails = ContactDetails(None, Some("07995447896"))

    val nonCustomerTriggered =
      generate[TriggeredV4].copy(metadata = generate[MetadataV3].copy(deliverTo = contactDetails))

    val orchestrationResult =
      Orchestrator[IO](SelectSMSChannel,
                       validCustomerProfile,
                       validContactProfile,
                       StubEmailOrchestrator,
                       StubSmsOrchestrator,
                       StubPrintOrchestrator)(nonCustomerTriggered, internalMetadata)

    val resultMetadata = orchestrationResult.unsafeRunSync().right.value
    resultMetadata shouldBe SMSOrchestratedMetadata
    contactDetailsPassedToChannelSelection shouldBe Some(
      ContactProfile(None, Some(MobilePhoneNumber("07995447896")), None))
    orchestratedDetails shouldBe Some(None, "07995447896")
    passedTriggered shouldBe Some(nonCustomerTriggered)
  }

  object SelectSMSChannel extends ChannelSelector[IO] {
    override def determineChannel(contactProfile: ContactProfile,
                                  customerPreferences: Seq[CommunicationPreference],
                                  triggered: TriggeredV4) = {
      contactDetailsPassedToChannelSelection = Some(contactProfile)
      IO.pure(Right(SMS))
    }
  }

  object SelectEmailChannel extends ChannelSelector[IO] {
    override def determineChannel(contactProfile: ContactProfile,
                                  customerPreferences: Seq[CommunicationPreference],
                                  triggered: TriggeredV4) = {
      contactDetailsPassedToChannelSelection = Some(contactProfile)
      IO.pure(Right(Email))
    }
  }

}
