package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.either._
import com.ovoenergy.comms.model.{ContactDetails, _}
import com.ovoenergy.orchestration.domain.{CommunicationPreference, EmailAddress, MobilePhoneNumber}
import com.ovoenergy.orchestration.kafka.IssueOrchestratedComm

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {

  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, domain.CustomerProfile],
            channelSelector: ChannelSelector,
            validateProfile: (domain.CustomerProfile) => Either[ErrorDetails, domain.CustomerProfile],
            issueOrchestratedEmail: IssueOrchestratedComm[EmailAddress],
            issueOrchestratedSMS: IssueOrchestratedComm[MobilePhoneNumber])(
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
      case contactDetails @ ContactDetails(_, _) =>
        for {
          contactProfile <- contactDetailsToContactProfile(contactDetails)
          res            <- orchestrate(triggered, contactProfile, Seq(), None)
        } yield res
    }
  }

  private def contactDetailsToContactProfile(
      contactDetails: ContactDetails): Either[ErrorDetails, domain.ContactProfile] = {
    val validatedPhoneNumber = contactDetails.phoneNumber.map(MobilePhoneNumber.create)
    val emailAddress         = contactDetails.emailAddress
    (emailAddress, validatedPhoneNumber) match {
      case (None, None)             => Left(ErrorDetails("No contact details found", InvalidProfile))
      case (email @ Some(_), None)  => Right(domain.ContactProfile(email.map(EmailAddress), None))
      case (None, Some(Left(e)))    => Left(ErrorDetails(e, InvalidProfile))
      case (_, Some(Left(_)))       => Right(domain.ContactProfile(emailAddress.map(EmailAddress), None))
      case (_, Some(Right(number))) => Right(domain.ContactProfile(emailAddress.map(EmailAddress), Some(number)))
    }
  }
}
