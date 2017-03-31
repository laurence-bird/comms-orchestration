package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.{Email, Post, SMS}
import com.ovoenergy.comms.model.ErrorCode.{OrchestrationError, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{CustomerDeliveryDetails, CustomerProfile}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbGenerator
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

  var passedCustomerProfile: Option[CustomerDeliveryDetails] = None
  var passedTriggered: Option[TriggeredV2]                   = None

  val customerProfile  = generate[CustomerProfile]
  val triggered        = generate[TriggeredV2]
  val internalMetadata = generate[InternalMetadata]

  val emailOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.email", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)
  val SMSOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.SMS", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)

  def emailOrchestrator =
    (customerDeliveryDetails: CustomerDeliveryDetails, triggered: TriggeredV2, internalMetadata: InternalMetadata) => {
      passedCustomerProfile = Some(customerDeliveryDetails)
      passedTriggered = Some(triggered)
      Future.successful(emailOrchestratedMetadata)
    }

  def smsOrchestrator =
    (customerDeliveryDetails: CustomerDeliveryDetails, triggered: TriggeredV2, internalMetadata: InternalMetadata) => {
      passedCustomerProfile = Some(customerDeliveryDetails)
      passedTriggered = Some(triggered)
      Future.successful(SMSOrchestratedMetadata)
    }

  private def customerProfiler(profile: CustomerProfile) =
    (customerId: String, canary: Boolean, traceToken: String) => {
      Right(profile)
    }

  val validProfile = (customerProfile: CustomerProfile) => Right(customerProfile)
  val invalidProfile = (customerProfile: CustomerProfile) =>
    Left(ErrorDetails("Name missing from profile", ErrorCode.InvalidProfile))

  override def beforeEach(): Unit = {
    passedCustomerProfile = None
    passedTriggered = None
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile, triggeredV2: TriggeredV2) => Right(Post)
    val orchestrator = //(CustomerProfile, TriggeredV2, InternalMetadata)
    Orchestrator(customerProfiler(customerProfile),
                 selectNonSupportedChannel,
                 validProfile,
                 emailOrchestrator,
                 smsOrchestrator)(triggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("Unsupported channel selected Post", OrchestrationError)
  }

  it should "handle failed channel selection" in {
    def failedChannelSelection =
      (customerProfile: CustomerProfile, triggeredV2: TriggeredV2) =>
        Left(ErrorDetails("whatever", OrchestrationError))
    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(customerProfiler(customerProfile),
                   failedChannelSelection,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(triggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle failed customer profiler" in {
    def selectEmailChannel = (customerProfile: CustomerProfile, triggeredV2: TriggeredV2) => Right(Email)
    def badCustomerProfiler: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile] =
      (customerId: String, canary: Boolean, traceToken: String) =>
        Left(ErrorDetails("whatever", ProfileRetrievalFailed))
    val orchestrator =
      Orchestrator(badCustomerProfiler, selectEmailChannel, validProfile, emailOrchestrator, smsOrchestrator)(
        triggered,
        internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle email channel" in {
    val profileWithEmailAddress = customerProfile.copy(emailAddress = Some("mrtest@testing.com"))
    def selectEmailChannel      = (customerProfile: CustomerProfile, triggeredV2: TriggeredV2) => Right(Email)
    val orchestrationResult =
      Orchestrator(customerProfiler(profileWithEmailAddress),
                   selectEmailChannel,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(triggered, internalMetadata)
    whenReady(orchestrationResult.right.value) { (resultMetadata: RecordMetadata) =>
      resultMetadata shouldBe emailOrchestratedMetadata
      passedCustomerProfile shouldBe Some(
        CustomerDeliveryDetails(profileWithEmailAddress.name, profileWithEmailAddress.emailAddress.get))
      passedTriggered shouldBe Some(triggered)
    }
  }

  it should "handle SMS channel" in {
    val profileWithPhoneNumber = customerProfile.copy(mobileNumber = Some("12345678"))
    def selectEmailChannel     = (customerProfile: CustomerProfile, triggeredV2: TriggeredV2) => Right(SMS)
    val orchestrator =
      Orchestrator(customerProfiler(profileWithPhoneNumber),
                   selectEmailChannel,
                   validProfile,
                   emailOrchestrator,
                   smsOrchestrator)(triggered, internalMetadata)
    whenReady(orchestrator.right.value) { resultMetadata =>
      resultMetadata shouldBe SMSOrchestratedMetadata
      passedCustomerProfile shouldBe Some(
        CustomerDeliveryDetails(profileWithPhoneNumber.name, profileWithPhoneNumber.mobileNumber.get))
      passedTriggered shouldBe Some(triggered)
    }
  }

}
