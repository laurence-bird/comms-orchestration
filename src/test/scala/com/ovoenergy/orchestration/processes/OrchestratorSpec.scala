package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.{OrchestrationError, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbGenerator
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Record
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._

import scala.concurrent.Future

class OrchestratorSpec
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with OneInstancePerTest
    with EitherValues
    with ArbGenerator {

  var passedCustomerProfile: CustomerProfile = _
  var passedTriggered: TriggeredV2           = _
  var invocationCount: Int                   = 0

  val recordMetadata = new RecordMetadata(new TopicPartition("test", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)

  def emailOrchestrator =
    (customerProfile: CustomerProfile, triggered: TriggeredV2, internalMetadata: InternalMetadata) => {
      passedCustomerProfile = customerProfile
      passedTriggered = triggered
      invocationCount = invocationCount + 1
      Right(Future.successful(recordMetadata))
    }

  val customerProfile  = generate[CustomerProfile]
  val triggered        = generate[TriggeredV2]
  val internalMetadata = generate[InternalMetadata]

  private def customerProfiler = (customerId: String, canary: Boolean, traceToken: String) => {
    Right(customerProfile)
  }

  behavior of "Orchestrator"

  it should "handle unsupported channels" in {
    def selectNonSupportedChannel = (customerProfile: CustomerProfile) => Right(SMS)
    val orchestrator = //(CustomerProfile, TriggeredV2, InternalMetadata)
    Orchestrator(customerProfiler, selectNonSupportedChannel, emailOrchestrator)(triggered, internalMetadata)

    orchestrator.left.value shouldBe ErrorDetails("Unsupported channel selected SMS", OrchestrationError)
  }

  it should "handle failed channel selection" in {
    def failedChannelSelection =
      (customerProfile: CustomerProfile) => Left(ErrorDetails("whatever", OrchestrationError))
    val orchestrator: Either[ErrorDetails, Future[RecordMetadata]] =
      Orchestrator(customerProfiler, failedChannelSelection, emailOrchestrator)(triggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", OrchestrationError)
  }

  it should "handle failed customer profiler" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    def badCustomerProfiler: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile] =
      (customerId: String, canary: Boolean, traceToken: String) =>
        Left(ErrorDetails("whatever", ProfileRetrievalFailed))
    val orchestrator =
      Orchestrator(badCustomerProfiler, selectEmailChannel, emailOrchestrator)(triggered, internalMetadata)
    orchestrator.left.value shouldBe ErrorDetails("whatever", ProfileRetrievalFailed)
  }

  it should "handle email channel" in {
    def selectEmailChannel = (customerProfile: CustomerProfile) => Right(Email)
    val orchestrator =
      Orchestrator(customerProfiler, selectEmailChannel, emailOrchestrator)(triggered, internalMetadata)
    whenReady(orchestrator.right.value) { result =>
      invocationCount shouldBe 1
      passedCustomerProfile shouldBe customerProfile
      passedTriggered shouldBe triggered
    }
  }

}
