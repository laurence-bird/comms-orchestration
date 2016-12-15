package com.ovoenergy.orchestration.processes.failure

import com.ovoenergy.comms.model.{Failed, Metadata, Triggered}

import scala.concurrent.Future

object Failure {

  def apply(failedProducer: (Failed) => Future[_])(reason: String, triggered: Triggered): Future[_] = {

    val event = Failed(
      metadata = Metadata.fromSourceMetadata("orchestration", triggered.metadata),
      reason = reason
    )

    failedProducer(event)
  }

}
