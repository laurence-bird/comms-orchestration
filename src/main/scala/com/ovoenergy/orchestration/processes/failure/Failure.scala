package com.ovoenergy.orchestration.processes.failure

import com.ovoenergy.comms.model._

import scala.concurrent.Future

object Failure {

  def apply(failedProducer: (Failed) => Future[_])(reason: String, triggered: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata): Future[_] = {

    val event = Failed(
      metadata = Metadata.fromSourceMetadata("orchestration", triggered.metadata),
      reason = reason,
      errorCode = errorCode,
      internalMetadata = internalMetadata
    )

    failedProducer(event)
  }

}
