package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.comms.model.Channel
import com.ovoenergy.orchestration.profile.CustomerProfiler.CustomerProfile

object ChannelSelector {

  def apply(customerProfile: CustomerProfile): Channel = {
    Email
  }

}
