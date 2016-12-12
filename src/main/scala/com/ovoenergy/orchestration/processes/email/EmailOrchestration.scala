package com.ovoenergy.orchestration.processes.email

import akka.Done
import com.ovoenergy.comms.model.Triggered
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

import scala.concurrent.Future

object EmailOrchestration {

  def apply()(customerProfile: CustomerProfile, triggered: Triggered): Future[Done] = {
    Future.successful(Done)
  }

}
