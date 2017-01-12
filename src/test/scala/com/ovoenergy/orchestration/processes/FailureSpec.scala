package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{Failed, InternalMetadata, Triggered}
import com.ovoenergy.orchestration.processes.failure.Failure
import org.scalacheck.Arbitrary
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

import scala.concurrent.Future

class FailureSpec extends FlatSpec
  with Matchers
  with ScalaFutures {

  private def generate[A](a: Arbitrary[A]) = {
    a.arbitrary.sample.get
  }

  var providedFailed: Failed = _
  val producer = (failed: Failed) => {
    providedFailed = failed
    Future.successful(Done)
  }

  behavior of "Failure process"

  it should "produced failed event" in {
    val internalMetaData = generate(implicitly[Arbitrary[InternalMetadata]])
    val triggered = generate(implicitly[Arbitrary[Triggered]])
    val future = Failure(producer)("Failure reason", triggered, OrchestrationError, internalMetaData)
    whenReady(future) { result =>
      providedFailed.reason shouldBe "Failure reason"
      providedFailed.metadata.traceToken shouldBe triggered.metadata.traceToken
      providedFailed.internalMetadata shouldBe internalMetaData
    }
  }


}
