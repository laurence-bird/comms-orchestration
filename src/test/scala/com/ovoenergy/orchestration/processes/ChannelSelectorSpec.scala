package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.Email
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class ChannelSelectorSpec extends FlatSpec
  with Matchers {

  val customerProfile = CustomerProfile(CustomerProfileName(Some("Mr"), "John", "Smith", None), CustomerProfileEmailAddresses(Some("some.email@ovoenergy.com"), None))

  behavior of "ChannelSelector"

  it should "always return Email" in {
    ChannelSelector.determineChannel(customerProfile) shouldBe Right(Email)
  }

}
