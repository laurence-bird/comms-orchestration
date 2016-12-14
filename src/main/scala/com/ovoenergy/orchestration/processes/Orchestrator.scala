package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.Main._
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

import scala.concurrent.Future

object Orchestrator {

  def apply(customerProfiler: (String) => CustomerProfile, channelSelector: (CustomerProfile) => Channel, emailOrchestrator: (CustomerProfile, Triggered) => Future[_])
           (triggered: Triggered): Future[_] = {

    def determineOrchestrator(channel: Channel): (CustomerProfile, Triggered) => Future[_] = {

      def unsupportedChannel = (customerProfile: CustomerProfile, triggered: Triggered) => {
        logError(triggered.metadata.traceToken, s"Unsupported channel selected $channel")
        Future.successful(Done)
      }

      channel match {
        case Email  => emailOrchestrator
        case _      => unsupportedChannel
      }
    }

    val customerProfile = customerProfiler(triggered.metadata.customerId)
    val channel = channelSelector(customerProfile)
    determineOrchestrator(channel)(customerProfile, triggered)
  }

}
