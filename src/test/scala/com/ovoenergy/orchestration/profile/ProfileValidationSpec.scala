package com.ovoenergy.orchestration.profile

import org.scalatest.{EitherValues, FlatSpec, Matchers}

class ProfileValidationSpec extends FlatSpec with Matchers with EitherValues {

  behavior of "getValidatedContactProfile"

  it should "return a valid profile if all but one contact method is valid" in {}

  it should "return a valid profile if all contact methods are valid" in {}

  it should "return appropriate error response if no contact details are provided" in {}

  it should "combine error messages if all contact methods are invalid" in {}

  behavior of "getValidatedCustomerProfile"

  it should "return appropriate error message if call to profiles service fails" in {}

  it should "return appropriate error message if customer name fails validation checks" in {}
//  it should "Validate a valid profile" in {
//    val goodCustomerProfile = CustomerProfile(
//      CustomerProfileName(Some("Mr"), "Stevie", "Wonder", Some("Esq")),
//      Seq.empty,
//      ContactProfile(
//        None,
//        None,
//        None
//      )
//    )
//
//    ProfileValidation(goodCustomerProfile).right.value shouldBe goodCustomerProfile
//  }
//
//  it should "Fail on an Invalid profile, specifying the parts missing" in {
//    val badCustomerProfile = CustomerProfile(
//      CustomerProfileName(Some("Mr"), "", "", Some("Esq")),
//      Seq.empty,
//      ContactProfile(
//        None,
//        None,
//        None
//      )
//    )
//    ProfileValidation(badCustomerProfile).left.value shouldBe ErrorDetails(
//      "Customer has no first name, Customer has no last name",
//      InvalidProfile)
//
//  }
//
//  it should "validate all the mobile phone numbers" in {
//    val validNo1 = "+447834774651"
//    val validNo2 = "07834774651"
//    val validNo3 = "+44 7834774651"
//    val validNo4 = "00447834774651"
//
//    ContactValidation.validateMobileNumber(validNo1) shouldBe Right(MobilePhoneNumber(validNo1))
//    ContactValidation.validateMobileNumber(validNo2) shouldBe Right(MobilePhoneNumber("+447834774651"))
//    ContactValidation.validateMobileNumber(validNo3) shouldBe Right(MobilePhoneNumber("+447834774651"))
//    ContactValidation.validateMobileNumber(validNo4) shouldBe Right(MobilePhoneNumber("+447834774651"))
//  }
//
//  it should "reject all the invalid phone numbers" in {
//    val invalidNo1 = "+441689826313"
//    val invalidNo2 = "02345670954538345"
//    val invalidNo3 = "+1-541-754-3010"
//    val invalidNo4 = "7321654321"
//
//    ContactValidation.validateMobileNumber(invalidNo1) shouldBe Left(
//      ErrorDetails("Invalid phone number provided", InvalidProfile))
//    ContactValidation.validateMobileNumber(invalidNo2) shouldBe Left(
//      ErrorDetails("Invalid phone number provided", InvalidProfile))
//    ContactValidation.validateMobileNumber(invalidNo3) shouldBe Left(
//      ErrorDetails("Invalid phone number provided", InvalidProfile))
//    ContactValidation.validateMobileNumber(invalidNo4) shouldBe Left(
//      ErrorDetails("Invalid phone number provided", InvalidProfile))
//  }

}
