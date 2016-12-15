package com.ovoenergy.orchestration.processes.email

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.{Apply, Semigroup}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{Metadata, OrchestratedEmail, Triggered}
import com.ovoenergy.orchestration.profile.CustomerProfiler._

import scala.concurrent.Future

object EmailOrchestration {

  case class ValidationError(message: String)
  case class ValidationErrors(errors: NonEmptyList[ValidationError]) {
    def errorsString: String = {
      errors.map(_.message).toList.mkString(", ")
    }
  }
  object ValidationErrors {
    def apply(message: String): ValidationErrors = ValidationErrors(ValidationError(message))
    def apply(error: ValidationError): ValidationErrors = ValidationErrors(NonEmptyList.of(error))
    implicit val sg = new Semigroup[ValidationErrors] {
      def combine(x: ValidationErrors, y: ValidationErrors): ValidationErrors = ValidationErrors(x.errors.concat(y.errors))
    }
  }
  private type ValidationErrorsOr[A] = Validated[ValidationErrors, A]

  def apply(orchestratedEmailProducer: (OrchestratedEmail) => Future[_])
           (customerProfile: CustomerProfile, triggered: Triggered): Future[_] = {

    val emailAddress: ValidationErrorsOr[String] = {
      customerProfile.emailAddresses match {
        case CustomerProfileEmailAddresses(primary, _) if !primary.isEmpty =>
          Validated.valid(primary)
        case CustomerProfileEmailAddresses(_, secondary) if !secondary.isEmpty =>
          Validated.valid(secondary)
        case _ =>
          Validated.invalid(ValidationErrors("Customer has no email address"))
      }
    }

    val firstName: ValidationErrorsOr[String] = {
      if (customerProfile.name.firstName.isEmpty) Validated.invalid(ValidationErrors("Customer has no first name"))
      else Validated.valid(customerProfile.name.firstName)
    }

    val lastName: ValidationErrorsOr[String] = {
      if (customerProfile.name.lastName.isEmpty) Validated.invalid(ValidationErrors("Customer has no last name"))
      else Validated.valid(customerProfile.name.lastName)
    }

    val resultOrValidationErrors: ValidationErrorsOr[Future[_]] =
      Apply[ValidationErrorsOr].map3(emailAddress, firstName, lastName) {
        case (validEmailAddress, validFirstName, validLastName) =>
          val orchestratedEmail = OrchestratedEmail(
            metadata = Metadata.fromSourceMetadata(
              source = "orchestration",
              sourceMetadata = triggered.metadata
            ),
            recipientEmailAddress = validEmailAddress,
            customerProfile = model.CustomerProfile(
              firstName = validFirstName,
              lastName = validLastName),
            templateData = triggered.templateData
          )

          orchestratedEmailProducer(orchestratedEmail)
      }

    resultOrValidationErrors match {
      case Valid(result) => result
      case Invalid(errors) => Future.failed(new Exception(errors.errorsString))
    }

  }

}
