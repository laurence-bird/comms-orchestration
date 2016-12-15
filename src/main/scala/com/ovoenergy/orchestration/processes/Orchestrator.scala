package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object Orchestrator extends LoggingWithMDC {

  def apply(customerProfiler: (String) => Try[CustomerProfile], channelSelector: (CustomerProfile) => Try[Channel], emailOrchestrator: (CustomerProfile, Triggered) => Future[_])
           (triggered: Triggered): Future[_] = {

    def determineOrchestrator(channel: Channel): Try[(CustomerProfile, Triggered) => Future[_]] = {
      channel match {
        case Email  => Success(emailOrchestrator)
        case _      => Failure(new Exception(s"Unsupported channel selected $channel"))
      }
    }

    val orchestratorTry = for {
      customerProfile <- customerProfiler(triggered.metadata.customerId)
      channel <- channelSelector(customerProfile)
      orchestrator <- determineOrchestrator(channel)
    } yield orchestrator(customerProfile, triggered)

    orchestratorTry match {
      case Success(orchestrator) => orchestrator
      case Failure(ex) =>
        logError(triggered.metadata.traceToken, "Error determining orchestrator to use", ex)
        Future.failed(ex)
    }
  }

  override def loggerName: String = "Orchestrator"
}
