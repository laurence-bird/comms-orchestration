package com.ovoenergy.orchestration.profile

import com.ovoenergy.comms.model.InvalidProfile
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.scalatest.{FlatSpec, Matchers}

class ContactValidationSpec extends FlatSpec with Matchers {

  it should "validate all the mobile phone numbers" in {
    val validNo1 = "+447834774651"
    val validNo2 = "07834774651"
    val validNo3 = "+44 7834774651"
    val validNo4 = "00447834774651"

    ContactValidation.validateMobileNumber(validNo1) shouldBe Right(MobilePhoneNumber(validNo1))
    ContactValidation.validateMobileNumber(validNo2) shouldBe Right(MobilePhoneNumber("+447834774651"))
    ContactValidation.validateMobileNumber(validNo3) shouldBe Right(MobilePhoneNumber("+447834774651"))
    ContactValidation.validateMobileNumber(validNo4) shouldBe Right(MobilePhoneNumber("+447834774651"))
  }

  it should "reject all the invalid phone numbers" in {
    val invalidNo1 = "+441689826313"
    val invalidNo2 = "02345670954538345"
    val invalidNo3 = "+1-541-754-3010"
    val invalidNo4 = "7321654321"

    ContactValidation.validateMobileNumber(invalidNo1) shouldBe Left(
      ErrorDetails("Invalid phone number provided", InvalidProfile))
    ContactValidation.validateMobileNumber(invalidNo2) shouldBe Left(
      ErrorDetails("Invalid phone number provided", InvalidProfile))
    ContactValidation.validateMobileNumber(invalidNo3) shouldBe Left(
      ErrorDetails("Invalid phone number provided", InvalidProfile))
    ContactValidation.validateMobileNumber(invalidNo4) shouldBe Left(
      ErrorDetails("Invalid phone number provided", InvalidProfile))
  }

}
