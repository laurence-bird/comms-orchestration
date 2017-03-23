package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{InternalMetadata, _}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  type Orchestrator = (CustomerProfile, TriggeredV2, InternalMetadata) => Either[ErrorDetails, Future[RecordMetadata]]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            determineChannel: (CustomerProfile, TriggeredV2) => Either[ErrorDetails, Channel],
            orchestrateEmail: Orchestrator,
            orchestrateSMS: Orchestrator)(
      triggered: TriggeredV2,
      internalMetadata: InternalMetadata): Either[ErrorDetails, Future[RecordMetadata]] = {

    def selectOrchestratorforChannel(channel: Channel): Either[ErrorDetails, Orchestrator] =
      channel match {
        case Email => Right(orchestrateEmail)
        case SMS   => Right(orchestrateSMS)
        case _     => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }

    for {
      customerProfile <- profileCustomer(triggered.metadata.customerId,
                                         triggered.metadata.canary,
                                         triggered.metadata.traceToken).right
      channel         <- determineChannel(customerProfile, triggered).right
      orchestrateComm <- selectOrchestratorforChannel(channel).right
      res             <- orchestrateComm(customerProfile, triggered, internalMetadata).right
    } yield res
  }
}
