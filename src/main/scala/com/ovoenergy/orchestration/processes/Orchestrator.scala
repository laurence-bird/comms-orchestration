package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{InternalMetadata, _}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC

import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  type Orchestrator = (CustomerProfile, TriggeredV2, InternalMetadata) => Either[ErrorDetails, Future[_]]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            determineChannel: (CustomerProfile) => Either[ErrorDetails, Channel],
            orchestrateEmail: Orchestrator)(triggered: TriggeredV2,
                                            internalMetadata: InternalMetadata): Either[ErrorDetails, Future[_]] = {

    def selectOrchestratorforChannel(channel: Channel): Either[ErrorDetails, Orchestrator] =
      channel match {
        case Email => Right(orchestrateEmail)
        case _     => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }

    for {
      customerProfile <- profileCustomer(triggered.metadata.customerId,
                                         triggered.metadata.canary,
                                         triggered.metadata.traceToken).right
      channel         <- determineChannel(customerProfile).right
      orchestrateComm <- selectOrchestratorforChannel(channel).right
      res             <- orchestrateComm(customerProfile, triggered, internalMetadata).right
    } yield res
  }

  override def loggerName: String = "Orchestrator"
}
