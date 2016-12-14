package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.Main._
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

import scala.concurrent.Future
import scala.util.control.NonFatal

object Orchestrator {

  def apply(customerProfiler: (String) => CustomerProfile, channelSelector: (CustomerProfile) => Channel, emailOrchestrator: (CustomerProfile, Triggered) => Future[_])
           (triggered: Triggered): Future[_] = {

    def determineOrchestrator(channel: Channel): (CustomerProfile, Triggered) => Future[_] = {
      channel match {
        case Email  => emailOrchestrator
        case _      => throw new Exception(s"Unsupported channel selected $channel")
      }
    }

    val customerProfile = customerProfiler(triggered.metadata.customerId)
    val channel = channelSelector(customerProfile)

    try {
      determineOrchestrator(channel)(customerProfile, triggered)
    } catch {
      case NonFatal(ex) =>
        logError(triggered.metadata.traceToken, "Error determining orchestrator to use", ex)
        Future.failed(ex)
    }
  }
}
