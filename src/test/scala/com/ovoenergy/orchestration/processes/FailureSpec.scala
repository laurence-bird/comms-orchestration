package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Failed
import com.ovoenergy.orchestration.processes.failure.Failure
import com.ovoenergy.orchestration.util.TestUtil
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future

class FailureSpec extends FlatSpec
  with Matchers
  with ScalaFutures {

  var providedFailed: Failed = _
  val producer = (failed: Failed) => {
    providedFailed = failed
    Future.successful(Done)
  }

  behavior of "Failure process"

  it should "produced failed event" in {
    val future = Failure(producer)("Failure reason", TestUtil.triggered)
    whenReady(future) { result =>
      providedFailed.reason shouldBe "Failure reason"
      providedFailed.metadata.traceToken shouldBe TestUtil.traceToken
    }
  }


}
