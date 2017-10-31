package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.either._
import cats.implicits._
import cats.kernel.{Monoid, Semigroup}
import com.ovoenergy.comms.model.{ContactDetails, _}
import com.ovoenergy.orchestration.domain.{
  CommunicationPreference,
  ContactAddress,
  ContactInfo,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.kafka.IssueOrchestratedComm
import com.ovoenergy.orchestration.profile.ContactValidation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {

  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, domain.CustomerProfile],
            channelSelector: ChannelSelector,
            validateProfile: (domain.CustomerProfile) => Either[ErrorDetails, domain.CustomerProfile],
            issueOrchestratedEmail: IssueOrchestratedComm[EmailAddress],
            issueOrchestratedSMS: IssueOrchestratedComm[MobilePhoneNumber],
            issueOrchestratedPrint: IssueOrchestratedComm[ContactAddress])(
      triggered: TriggeredV3,
      internalMetadata: InternalMetadata): Either[ErrorDetails, Future[RecordMetadata]] = {

    def issueOrchestratedComm(customerProfile: Option[model.CustomerProfile],
                              channel: Channel,
                              contactProfile: domain.ContactProfile): Either[ErrorDetails, Future[RecordMetadata]] = {
      channel match {
        case Email =>
          contactProfile.emailAddress
            .map(issueOrchestratedEmail.send(customerProfile, _, triggered))
            .toRight {
              val errorDetails = "Phone number missing from customer profile"
              logWarn(triggered.metadata.traceToken, errorDetails)
              ErrorDetails(errorDetails, InvalidProfile)
            }
        case SMS =>
          contactProfile.mobileNumber
            .map(issueOrchestratedSMS.send(customerProfile, _, triggered))
            .toRight {
              logWarn(triggered.metadata.traceToken, "Phone number missing from customer profile")
              ErrorDetails("No valid phone number provided", InvalidProfile)
            }

        case Print =>
          contactProfile.postalAddress
            .map(ContactAddress.fromCustomerAddress)
            .map(issueOrchestratedPrint.send(customerProfile, _, triggered))
            .toRight {
              logWarn(triggered.metadata.traceToken, "Customer address missing from customer profile")
              ErrorDetails("No valid postal address provided", InvalidProfile)
            }

        case _ => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }
    }

    def orchestrate(triggeredV3: TriggeredV3,
                    contactProfile: domain.ContactProfile,
                    customerPreferences: Seq[CommunicationPreference],
                    customerProfile: Option[model.CustomerProfile]) = {
      for {
        channel <- channelSelector.determineChannel(contactProfile, customerPreferences, triggeredV3)
        res     <- issueOrchestratedComm(customerProfile, channel, contactProfile)
      } yield res
    }

    def retrieveCustomerProfile(customerId: String,
                                triggeredV3: TriggeredV3): Either[ErrorDetails, domain.CustomerProfile] = {
      for {
        customerProfile  <- profileCustomer(customerId, triggered.metadata.canary, triggered.metadata.traceToken)
        validatedProfile <- validateProfile(customerProfile)
      } yield validatedProfile
    }

    triggered.metadata.deliverTo match {
      case Customer(customerId) => {
        for {
          customerProfile <- retrieveCustomerProfile(customerId, triggered)
          res <- orchestrate(triggered,
                             customerProfile.contactProfile,
                             customerProfile.communicationPreferences,
                             Some(customerProfile.toModel))
        } yield res
      }
      case contactDetails @ ContactDetails(_, _, _) =>
        for {
          contactProfile <- contactDetailsToContactProfile(contactDetails)
          res            <- orchestrate(triggered, contactProfile, Seq(), None)
        } yield res
    }
  }

  private def contactDetailsToContactProfile(
      contactDetails: ContactDetails): Either[ErrorDetails, domain.ContactProfile] = {
    val validatedPhoneNumber: Option[Either[ErrorDetails, MobilePhoneNumber]] =
      contactDetails.phoneNumber.map(ContactValidation.validateMobileNumber)
    val emailAddress = contactDetails.emailAddress.map(e => Right(EmailAddress(e)))
    val validatedPostalAddress: Option[Either[ErrorDetails, ContactAddress]] = {
      contactDetails.postalAddress
        .map(ContactAddress.fromCustomerAddress)
        .map(ContactValidation.validatePostalAddress)
    }

    val detailsList: Seq[Either[ErrorDetails, ContactInfo]] =
      List(validatedPhoneNumber, emailAddress, validatedPostalAddress).flatten

    case class ValidatedContactDetails(invalidDetails: List[ErrorDetails], validDetails: List[ContactInfo])

    val result = detailsList.foldLeft(ValidatedContactDetails(Nil, Nil)) {
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
}
