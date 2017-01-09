package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{Channel, ErrorCode, Triggered}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC

import scala.concurrent.Future
import scala.util.Either.RightProjection

object Orchestrator extends LoggingWithMDC {
  case class ErrorStuff(reason: String, errorCode: ErrorCode)

  type Orchestrator = (CustomerProfile, Triggered) => Either[ErrorStuff, Future[_]]

  def apply(customerProfiler: (String, Boolean, String) => Either[ErrorStuff, CustomerProfile],
            channelSelector: (CustomerProfile) => Either[ErrorStuff, Channel],
            emailOrchestrator: Orchestrator)
           (triggered: Triggered): Either[ErrorStuff, Future[_]] = {

    def determineOrchestrator(profile: CustomerProfile): Either[ErrorStuff, Orchestrator] = {
    val channel: Either[ErrorStuff, Channel] = channelSelector(profile)
      channel.right.flatMap[ErrorStuff, Orchestrator] { (ch: Channel) => { ch match {
        case Email  => Right(emailOrchestrator)
        case _      => Left(ErrorStuff(s"Unsupported channel selected $ch", OrchestrationError))
          }
        }
      }
    }

    for {
      customerProfile   <- customerProfiler(triggered.metadata.customerId, triggered.metadata.canary, triggered.metadata.traceToken).right
      orchestratorFunc  <- determineOrchestrator(customerProfile).right
      res               <- orchestratorFunc(customerProfile, triggered).right
    } yield res
  }

  override def loggerName: String = "Orchestrator"
}