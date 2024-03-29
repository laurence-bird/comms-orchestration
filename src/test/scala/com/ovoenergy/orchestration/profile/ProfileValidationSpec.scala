package com.ovoenergy.orchestration.profile

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.{
  ContactProfile,
  CustomerProfile,
  CustomerProfileName,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.profile.CustomerProfiler.ProfileCustomer
import com.ovoenergy.orchestration.util.ArbInstances
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import org.scalacheck.Shapeless._

class ProfileValidationSpec extends FlatSpec with Matchers with EitherValues with ArbInstances {

  behavior of "getValidatedContactProfile"

  val validPhoneNumber           = Some(MobilePhoneNumber("07710036644"))
  val validPhoneNumberWithPrefix = Some(MobilePhoneNumber("+447710036644"))
  val validEmail                 = Some(EmailAddress("mrtest@gmail.com"))
  val validAddress = Some(
    CustomerAddress(line1 = "33 Notting Hill Gate",
                    line2 = Some(""),
                    town = "Kensington",
                    county = Some("London"),
                    postcode = "W11 3JQ",
                    country = Some("UK")))
  val invalidAddress     = Some(CustomerAddress("", None, "", None, "", None))
  val invalidPhoneNumber = Some(MobilePhoneNumber("5434575386734786345786345786354"))

  val validCustomerProfile = Right(
    CustomerProfile(CustomerProfileName(Some("Mr"), "Laurence", "Tureaud", None),
                    Nil,
                    ContactProfile(validEmail, validPhoneNumber, validAddress)))

  def buildProfileRetriever(response: Either[ErrorDetails, CustomerProfile]) = {
    new CustomerProfiler[IO] {
      override def apply(profileCustomer: ProfileCustomer): IO[Either[ErrorDetails, CustomerProfile]] =
        IO.pure(response)
    }
  }

  val customerProfile = generate[CustomerProfile]

  it should "return a valid profile if all but one contact method is invalid" in {
    val contactProfile = ContactProfile(validEmail, invalidPhoneNumber, None)
    ProfileValidation.validateContactProfile(contactProfile) shouldBe Right(
      ContactProfile(Some(EmailAddress("mrtest@gmail.com")), None, None))
  }

  it should "return a valid profile if all contact methods are valid" in {
    val contactProfile = ContactProfile(validEmail, validPhoneNumber, validAddress)
    ProfileValidation.validateContactProfile(contactProfile) shouldBe Right(
      contactProfile.copy(mobileNumber = validPhoneNumberWithPrefix))
  }

  it should "return appropriate error response if no contact details are provided" in {
    val contactProfile = ContactProfile(None, None, None)
    ProfileValidation.validateContactProfile(contactProfile) shouldBe Left(
      ErrorDetails("No contact details found", InvalidProfile))
  }

  it should "combine error messages if all contact methods are invalid" in {
    val contactProfile = ContactProfile(None, invalidPhoneNumber, invalidAddress)
    ProfileValidation.validateContactProfile(contactProfile) shouldBe Left(
      ErrorDetails("Invalid phone number provided, postcode: incorrect postcode format", InvalidProfile))
  }

  behavior of "getValidatedCustomerProfile"

  it should "return appropriate error message if call to profiles service fails" in {
    val errorResponse    = Left(ErrorDetails("Oh no it failed!", ProfileRetrievalFailed))
    val profileRetriever = buildProfileRetriever(errorResponse)
    val triggered        = generate[TriggeredV4]
    val customer         = generate[Customer]

    ProfileValidation
      .getValidatedCustomerProfile[IO](profileRetriever)(triggered, customer)
      .unsafeRunSync() shouldBe errorResponse
  }

  it should "return appropriate error message if customer name fails validation checks" in {
    val invalidNameProfile =
      validCustomerProfile.right.map(p => p.copy(name = CustomerProfileName(None, "", "", None)))
    val profileRetriever = buildProfileRetriever(invalidNameProfile)
    val triggered        = generate[TriggeredV4]
    val customer         = generate[Customer]

    ProfileValidation.getValidatedCustomerProfile(profileRetriever)(triggered, customer).unsafeRunSync() shouldBe Left(
      ErrorDetails("Customer has no first name, Customer has no last name", InvalidProfile))
  }

  it should "return a valid profile if all retrieval and profile validation is successful" in {
    val profileRetriever = buildProfileRetriever(validCustomerProfile)
    val triggered        = generate[TriggeredV4]
    val customer         = generate[Customer]

    ProfileValidation
      .getValidatedCustomerProfile(profileRetriever)(triggered, customer)
      .unsafeRunSync() shouldBe validCustomerProfile
  }

}
