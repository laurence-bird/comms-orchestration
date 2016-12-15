package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.orchestration.profile.CustomerProfiler.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class ChannelSelectorSpec extends FlatSpec
  with Matchers {

  val customerProfile = CustomerProfile(CustomerProfileName("Mr", "John", "Smith", ""), CustomerProfileEmailAddresses("some.email@ovoenergy.com", ""))

  behavior of "ChannelSelector"

  it should "always return Email" in {
    ChannelSelector.determineChannel(customerProfile) shouldBe Success(Email)
  }

}
