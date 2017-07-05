package com.ovoenergy.orchestration.domain

import org.scalatest.{FlatSpec, Matchers}

class MobilePhoneNumberSpec extends FlatSpec with Matchers {

  it should "validate all the mobile phone numbers" in {
    val validNo1 = "+447834774651"
    val validNo2 = "07834774651"
    val validNo3 = "+44 7834774651"
    val validNo4 = "00447834774651"

    MobilePhoneNumber.create(validNo1) shouldBe Right(MobilePhoneNumber(validNo1))
    MobilePhoneNumber.create(validNo2) shouldBe Right(MobilePhoneNumber("+447834774651"))
    MobilePhoneNumber.create(validNo3) shouldBe Right(MobilePhoneNumber("+447834774651"))
    MobilePhoneNumber.create(validNo4) shouldBe Right(MobilePhoneNumber("+447834774651"))
  }

  it should "reject all the invalid phone numbers" in {
    val invalidNo1 = "+441689826313"
    val invalidNo2 = "02345670954538345"
    val invalidNo3 = "+1-541-754-3010"
    val invalidNo4 = "7321654321"

    MobilePhoneNumber.create(invalidNo1) shouldBe Left("Invalid phone number provided")
    MobilePhoneNumber.create(invalidNo2) shouldBe Left("Invalid phone number provided")
    MobilePhoneNumber.create(invalidNo3) shouldBe Left("Invalid phone number provided")
    MobilePhoneNumber.create(invalidNo4) shouldBe Left("Invalid phone number provided")
  }

}
