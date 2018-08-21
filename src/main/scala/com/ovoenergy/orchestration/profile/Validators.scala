package com.ovoenergy.comms.orchestration.profile

import cats.data.{NonEmptyList, Validated}
import cats.data.Validated.{Invalid, Valid}
import com.ovoenergy.comms.orchestration.profile.AddressValidator.AddressLine
import cats.implicits._
object Validators {

  val isValidPostcode =
    validate((line: AddressLine) =>
               line.value.toUpperCase.matches("[A-Z]{1,2}[0-9]{1,2}[A-Z]{0,1}[ \t\r\n\f]{0,1}[0-9][A-Z]{2}"),
             "incorrect postcode format")(_)

  val atLeast2Char: (ValidationErrorsOr[AddressLine]) => Validated[NonEmptyList[String], AddressLine] =
    validate((line: AddressLine) => line.value.length > 1, "is less then 2 chars long")(_)

  val atLeast3Char: (ValidationErrorsOr[AddressLine]) => Validated[NonEmptyList[String], AddressLine] =
    validate((line: AddressLine) => line.value.length > 2, "is less then 3 chars long")(_)

  val atLeast1Number = validate((line: AddressLine) => line.value.matches(".*[0-9].*"), "does not contain digit")(_)

  val onlyNumber = validate((line: AddressLine) => line.value.matches("[0-9]*"), "should only contain numbers")(_)

  val onlyLetter =
    validate((line: AddressLine) => line.value.toUpperCase.matches("[^0-9]*"), "should only contain numbers")(_)

  val noRestriction = (i: ValidationErrorsOr[AddressLine]) => Valid(i)

  def validate(test: AddressLine => Boolean, message: String) = { (i: ValidationErrorsOr[AddressLine]) =>
    i match {
      case Valid(addressLine: AddressLine) if (test(addressLine)) => Valid(addressLine)
      case Valid(addressLine: AddressLine) if (!test(addressLine)) =>
        Invalid(NonEmptyList.of(s"${addressLine.name}: $message"))
      case _ => Invalid(NonEmptyList.of(message))
    }
  }

  type ValidationErrorsOr[A] = Validated[NonEmptyList[String], A]
}
