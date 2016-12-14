package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

object ChannelSelector {

  def determineChannel = (customerProfile: CustomerProfile) => {
    Email
  }

}
