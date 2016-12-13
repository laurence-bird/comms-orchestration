package com.ovoenergy.orchestration.processes

import akka.Done
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.{Channel, Triggered}
import com.ovoenergy.orchestration.Main._
import com.ovoenergy.orchestration.profile.CustomerProfiler
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

import scala.concurrent.Future

object Orchestrator {

  def apply(emailOrchestrator: (CustomerProfile, Triggered) => Future[_])(triggered: Triggered) = {

    def determineOrchestrator(channel: Channel): (CustomerProfile, Triggered) => Future[_] = {
      channel match {
        case Email =>
          emailOrchestrator
        case _ =>
          (customerProfile: CustomerProfile, triggered: Triggered) =>
            logError(triggered.metadata.traceToken, s"Unsupported channel selected $channel")
            Future.successful(Done)
      }
    }

    val customerProfile = CustomerProfiler(triggered.metadata.customerId)
    val orchestrator = customerProfile.map(ChannelSelector(_)).map(determineOrchestrator(_))

    for {
      customerProfile <- customerProfile
      orchestrator <- orchestrator
    } yield orchestrator(customerProfile, triggered)
  }

}
