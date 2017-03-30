package com.ovoenergy.orchestration.profile

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.{Apply, Semigroup}
import com.ovoenergy.comms.model.ErrorCode.InvalidProfile
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

object ProfileValidation extends LoggingWithMDC {

  case class ValidationError(message: String)
  case class ValidationErrors(errors: NonEmptyList[ValidationError]) {
    def errorsString: String = {
      errors.map(_.message).toList.mkString(", ")
    }
  }
  object ValidationErrors {
    def apply(message: String): ValidationErrors        = ValidationErrors(ValidationError(message))
    def apply(error: ValidationError): ValidationErrors = ValidationErrors(NonEmptyList.of(error))
    implicit val sg = new Semigroup[ValidationErrors] {
      def combine(x: ValidationErrors, y: ValidationErrors): ValidationErrors =
        ValidationErrors(x.errors.concat(y.errors))
    }
  }
  private type ValidationErrorsOr[A] = Validated[ValidationErrors, A]

  def apply(customerProfile: CustomerProfile): Either[ErrorDetails, CustomerProfile] = {
    val firstName: ValidationErrorsOr[String] = {
      if (customerProfile.name.firstName.isEmpty) Validated.invalid(ValidationErrors("Customer has no first name"))
      else Validated.valid(customerProfile.name.firstName)
    }

    val lastName: ValidationErrorsOr[String] = {
      if (customerProfile.name.lastName.isEmpty) Validated.invalid(ValidationErrors("Customer has no last name"))
      else Validated.valid(customerProfile.name.lastName)
    }

    val profileOrErrors = Apply[ValidationErrorsOr].map2(firstName, lastName) {
      case (validFirstName, validLastName) =>
        customerProfile
    }

    profileOrErrors match {
      case Valid(profile) => Right(profile)
      case Invalid(errors) =>
        Left(ErrorDetails(errors.errorsString, InvalidProfile))
    }
  }
}
