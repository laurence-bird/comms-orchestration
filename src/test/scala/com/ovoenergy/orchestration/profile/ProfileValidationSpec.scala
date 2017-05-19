package com.ovoenergy.orchestration.profile

import com.ovoenergy.comms.model.InvalidProfile
import com.ovoenergy.orchestration.domain.customer.{ContactProfile, CustomerProfile, CustomerProfileName}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class ProfileValidationSpec extends FlatSpec with Matchers with EitherValues {

  behavior of "ProfileValidation"

  it should "Validate a valid profile" in {
    val goodCustomerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "Stevie", "Wonder", Some("Esq")),
      Seq.empty,
      ContactProfile(
        None,
        None
      )
    )

    ProfileValidation(goodCustomerProfile).right.value shouldBe goodCustomerProfile
  }

  it should "Fail on an Invalid profile, specifying the parts missing" in {
    val badCustomerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "", "", Some("Esq")),
      Seq.empty,
      ContactProfile(
        None,
        None
      )
    )
    ProfileValidation(badCustomerProfile).left.value shouldBe ErrorDetails(
      "Customer has no first name, Customer has no last name",
      InvalidProfile)

  }

}
