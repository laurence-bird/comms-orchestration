package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel
import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.orchestration.domain.customerProfile.CustomerProfile

import scala.util.{Success, Try}

object ChannelSelector {

  def determineChannel(customerProfile: CustomerProfile): Try[Channel] = {
    Success(Email)
  }

}
