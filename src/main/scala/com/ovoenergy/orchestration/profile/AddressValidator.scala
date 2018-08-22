package com.ovoenergy.comms.orchestration.profile

import com.ovoenergy.comms.orchestration.profile.Validators.ValidationErrorsOr
import Validators._
import cats.data.Validated.Valid
import cats.Apply
import com.ovoenergy.comms.orchestration.domain

object AddressValidator {

  val line1Test    = noRestriction
  val townTest     = onlyLetter
  val postcodeTest = isValidPostcode
  val countyTest   = onlyLetter

  case class AddressLine(name: String, value: String)

  implicit def string2Line(addressLine: AddressLine): ValidationErrorsOr[AddressLine] = Valid(addressLine)

  def validateAddress(address: domain.ContactAddress) = {
    val line1    = line1Test(AddressLine("line 1", address.line1))
    val town     = townTest(AddressLine("town", address.town))
    val postcode = postcodeTest(AddressLine("postcode", address.postcode))
    val county   = address.county.map(c => countyTest(AddressLine("county", c))).getOrElse(Valid(()))

    Apply[ValidationErrorsOr].map4(
      line1,
      town,
      postcode,
      county
    ) {
      case (_, _, _, _) => address
    }
  }

}
