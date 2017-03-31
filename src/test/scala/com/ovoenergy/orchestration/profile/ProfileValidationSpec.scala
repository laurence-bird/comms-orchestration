package com.ovoenergy.orchestration.profile

import com.ovoenergy.comms.model.ErrorCode
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileName}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class ProfileValidationSpec extends FlatSpec with Matchers with EitherValues {

  behavior of "ProfileValidation"

  it should "Validate a valid profile" in {
    val goodCustomerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "Stevie", "Wonder", Some("Esq")),
      None,
      None,
      Seq.empty
    )

    ProfileValidation(goodCustomerProfile).right.value shouldBe goodCustomerProfile
  }

  it should "Fail on an Invalid profile, specifying the parts missing" in {
    val badCustomerProfile = CustomerProfile(
      CustomerProfileName(Some("Mr"), "", "", Some("Esq")),
      None,
      None,
      Seq.empty
    )
    ProfileValidation(badCustomerProfile).left.value shouldBe ErrorDetails(
      "Customer has no first name, Customer has no last name",
      ErrorCode.InvalidProfile)

  }

}
