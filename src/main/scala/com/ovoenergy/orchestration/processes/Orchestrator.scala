package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{Channel, ErrorCode, InternalMetadata, Triggered}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC

import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  type Orchestrator = (CustomerProfile, Triggered, InternalMetadata) => Either[ErrorDetails, Future[_]]

  def apply(customerProfiler: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            channelSelector: (CustomerProfile) => Either[ErrorDetails, Channel],
            emailOrchestrator: Orchestrator)
           (triggered: Triggered, internalMetadata: InternalMetadata): Either[ErrorDetails, Future[_]] = {

    def selectOrchestratorforChannel(channel: Channel): Either[ErrorDetails, Orchestrator] =
      channel match {
        case Email => Right(emailOrchestrator)
        case _ => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }

    for {
      customerProfile   <- customerProfiler(triggered.metadata.customerId, triggered.metadata.canary, triggered.metadata.traceToken).right
      channel           <- channelSelector(customerProfile).right
      orchestrator      <- selectOrchestratorforChannel(channel).right
      res               <- orchestrator(customerProfile, triggered, internalMetadata).right
    } yield res
  }

  override def loggerName: String = "Orchestrator"
}