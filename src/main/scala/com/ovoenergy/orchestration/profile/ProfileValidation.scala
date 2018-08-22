package com.ovoenergy.comms.orchestration.profile

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.effect.Async
import cats.{Apply, Monoid, Semigroup}
import cats.implicits._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.domain
import com.ovoenergy.comms.orchestration.domain.{
  ContactAddress,
  ContactInfo,
  ContactProfile,
  CustomerProfile,
  CustomerProfileName,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.comms.orchestration.logging.LoggingWithMDC
import com.ovoenergy.comms.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.comms.orchestration.profile.CustomerProfiler.ProfileCustomer

object ProfileValidation extends LoggingWithMDC {

  def getValidatedCustomerProfile[F[_]: Async](
      retrieveCustomerProfile: ProfileCustomer => F[Either[ErrorDetails, CustomerProfile]])(
      triggered: TriggeredV4,
      customer: Customer): F[Either[ErrorDetails, domain.CustomerProfile]] = {

    val customerProfileF = retrieveCustomerProfile(
      ProfileCustomer(customer.customerId, triggered.metadata.canary, triggered.metadata.traceToken))

    customerProfileF.map { cp =>
      for {
        customerProfile <- cp
        _               <- validateProfileName(customerProfile.name)
        _               <- validateContactProfile(customerProfile.contactProfile)
      } yield customerProfile
    }
  }

  private val validUkMobileRegex = """^\+447\d{9}$""".r
  private val phoneNumberPrefix  = """^(07|447|0{1,2}447)"""

  private def validateMobileNumber(number: String): Either[ErrorDetails, MobilePhoneNumber] = {
    val strippedPhoneNumber = number.trim
      .replaceAll("[^0-9]", "")
      .replaceFirst(phoneNumberPrefix, """+447""")
    validUkMobileRegex
      .findFirstIn(strippedPhoneNumber)
      .map(MobilePhoneNumber(_))
      .toRight(ErrorDetails("Invalid phone number provided", InvalidProfile))
  }

  private def validatePostalAddress(address: domain.ContactAddress): Either[ErrorDetails, domain.ContactAddress] = {
    AddressValidator
      .validateAddress(address)
      .toEither
      .left
      .map(errors => ErrorDetails(errors.toList.mkString(", "), InvalidProfile))
  }

  def validateContactProfile(contactProfile: ContactProfile): Either[ErrorDetails, domain.ContactProfile] = {
    /*
      1 - Validate mobile
      2 - validate email
      3 - validate postal address
      4 - if at least one defined, return valid profile
     */
    val validatedPhoneNumber: Option[Either[ErrorDetails, MobilePhoneNumber]] =
      contactProfile.mobileNumber.map(m => validateMobileNumber(m.number))

    val emailAddress = contactProfile.emailAddress.map(e => Right(EmailAddress(e.address)))
    val validatedPostalAddress: Option[Either[ErrorDetails, ContactAddress]] = {
      contactProfile.postalAddress
        .map(ContactAddress.fromCustomerAddress)
        .map(validatePostalAddress)
    }

    val detailsList: Seq[Either[ErrorDetails, ContactInfo]] =
      List(validatedPhoneNumber, emailAddress, validatedPostalAddress).flatten

    case class ValidatedContactDetails(invalidDetails: List[ErrorDetails], validDetails: List[ContactInfo])

    val result: ValidatedContactDetails = detailsList.foldLeft(ValidatedContactDetails(Nil, Nil)) {
      (acc: ValidatedContactDetails, element: Either[ErrorDetails, ContactInfo]) =>
        element match {
          case Left(e: ErrorDetails) => acc.copy(invalidDetails = acc.invalidDetails :+ e)
          case Right(r)              => acc.copy(validDetails = acc.validDetails :+ r)
        }
    }

    result match {
      case ValidatedContactDetails(Nil, Nil)    => Left(ErrorDetails("No contact details found", InvalidProfile))
      case ValidatedContactDetails(errors, Nil) => Left(combineErrors(errors))
      case ValidatedContactDetails(_, details)  => Right(buildContactProfile(details))
    }
  }

  private def buildContactProfile(contactInfo: List[ContactInfo]): domain.ContactProfile = {
    contactInfo.foldLeft(domain.ContactProfile(None, None, None)) { (acc, elem) =>
      elem match {
        case MobilePhoneNumber(sms) => acc.copy(mobileNumber = Some(MobilePhoneNumber(sms)))
        case EmailAddress(email)    => acc.copy(emailAddress = Some(EmailAddress(email)))
        case ContactAddress(l1, l2, town, county, postcode, country) =>
          acc.copy(postalAddress = Some(CustomerAddress(l1, l2, town, county, postcode, country)))
      }
    }
  }

  private def combineErrors(errors: List[ErrorDetails]) = {
    implicit val mon = new Monoid[ErrorDetails] {
      override def empty = ErrorDetails("", InvalidProfile)

      override def combine(x: ErrorDetails, y: ErrorDetails) = {
        if (x.reason.isEmpty) {
          ErrorDetails(y.reason, y.errorCode)
        } else {
          ErrorDetails(s"${x.reason}, ${y.reason}", x.errorCode)
        }
      }
    }

    errors.foldMap(identity)
  }

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
      override def combine(x: ValidationErrors, y: ValidationErrors): ValidationErrors =
        ValidationErrors(x.errors.concatNel(y.errors))
    }
  }
  private type ValidationErrorsOr[A] = Validated[ValidationErrors, A]

  private def validateProfileName(
      customerProfileName: CustomerProfileName): Either[ErrorDetails, CustomerProfileName] = {
    val firstName: ValidationErrorsOr[String] = {
      if (customerProfileName.firstName.isEmpty) Validated.invalid(ValidationErrors("Customer has no first name"))
      else Validated.valid(customerProfileName.firstName)
    }

    val lastName: ValidationErrorsOr[String] = {
      if (customerProfileName.lastName.isEmpty) Validated.invalid(ValidationErrors("Customer has no last name"))
      else Validated.valid(customerProfileName.lastName)
    }

    val profileOrErrors = Apply[ValidationErrorsOr].map2(firstName, lastName) {
      case (validFirstName, validLastName) =>
        customerProfileName
    }

    profileOrErrors match {
      case Valid(profileName) => Right(profileName)
      case Invalid(errors) =>
        Left(ErrorDetails(errors.errorsString, InvalidProfile))
    }
  }

}
