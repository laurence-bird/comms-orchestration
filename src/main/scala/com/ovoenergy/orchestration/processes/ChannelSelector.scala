package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

object ChannelSelector {

  def determineChannel(customerProfile: CustomerProfile): Either[ErrorDetails, Channel] = {

    Right(Email)
  }

}
