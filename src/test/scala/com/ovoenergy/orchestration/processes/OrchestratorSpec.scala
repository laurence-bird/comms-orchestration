package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.{Email, Post, SMS}
import com.ovoenergy.comms.model.ErrorCode.{OrchestrationError, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
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

  var passedCustomerProfile: Option[CustomerProfile] = None
  var passedTriggered: Option[TriggeredV2]           = None

  val customerProfile  = generate[CustomerProfile]
  val triggered        = generate[TriggeredV2]
  val internalMetadata = generate[InternalMetadata]

  val emailOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.email", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)
  val SMSOrchestratedMetadata =
    new RecordMetadata(new TopicPartition("comms.orchestrated.SMS", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)

  def emailOrchestrator =
    (customerProfile: CustomerProfile, triggered: TriggeredV2, internalMetadata: InternalMetadata) => {
      passedCustomerProfile = Some(customerProfile)
      passedTriggered = Some(triggered)
      Right(Future.successful(emailOrchestratedMetadata))
    }

  def smsOrchestrator =
    (customerProfile: CustomerProfile, triggered: TriggeredV2, internalMetadata: InternalMetadata) => {
      passedCustomerProfile = Some(customerProfile)
      passedTriggered = Some(triggered)
      Right(Future.successful(SMSOrchestratedMetadata))
    }

  private def customerProfiler = (customerId: String, canary: Boolean, traceToken: String) => {
    Right(customerProfile)
  }

  override def beforeEach(): Unit = {
    passedCustomerProfile = None
    passedTriggered = None
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile) => Right(Post)
    val orchestrator = //(CustomerProfile, TriggeredV2, InternalMetadata)
    Orchestrator(customerProfiler, selectNonSupportedChannel, emailOrchestrator, smsOrchestrator)(triggered,
                                                                                                  internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("Unsupported channel selected Post", OrchestrationError)
  }

  it should "handle failed channel selection" in {
    def failedChannelSelection =
      (customerProfile: CustomerProfile) => Left(ErrorDetails("whatever", OrchestrationError))
    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(customerProfiler, failedChannelSelection, emailOrchestrator, smsOrchestrator)(triggered,
                                                                                                 internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle failed customer profiler" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    def badCustomerProfiler: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile] =
      (customerId: String, canary: Boolean, traceToken: String) =>
        Left(ErrorDetails("whatever", ProfileRetrievalFailed))
    val orchestrator =
      Orchestrator(badCustomerProfiler, selectEmailChannel, emailOrchestrator, smsOrchestrator)(triggered,
                                                                                                internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle email channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    val orchestrator =
      Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator, smsOrchestrator)(triggered,
                                                                                             internalMetadata)
    whenReady(orchestrator.right.value) { resultMetadata =>
      resultMetadata shouldBe emailOrchestratedMetadata
      passedCustomerProfile shouldBe Some(customerProfile)
      passedTriggered shouldBe Some(triggered)
    }
  }

  it should "handle SMS channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(SMS)
    val orchestrator =
      Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator, smsOrchestrator)(triggered,
                                                                                             internalMetadata)
    whenReady(orchestrator.right.value) { resultMetadata =>
      resultMetadata shouldBe SMSOrchestratedMetadata
      passedCustomerProfile shouldBe Some(customerProfile)
      passedTriggered shouldBe Some(triggered)
    }
  }

}
