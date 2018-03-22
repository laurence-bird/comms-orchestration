package com.ovoenergy.orchestration.processes

import cats.effect.Async
import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import cats.implicits._
import com.ovoenergy.comms.model.{ContactDetails, _}
import com.ovoenergy.orchestration.domain.{
  CommunicationPreference,
  ContactAddress,
  ContactProfile,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.kafka.IssueOrchestratedComm

object Orchestrator extends LoggingWithMDC {

  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  def apply[F[_]: Async](
      channelSelector: ChannelSelector,
      getValidatedCustomerProfile: (TriggeredV3, Customer) => Either[ErrorDetails, domain.CustomerProfile],
      getValidatedContactProfile: ContactProfile => Either[ErrorDetails, ContactProfile],
      issueOrchestratedEmail: IssueOrchestratedComm[EmailAddress],
      issueOrchestratedSMS: IssueOrchestratedComm[MobilePhoneNumber],
      issueOrchestratedPrint: IssueOrchestratedComm[ContactAddress])(
      triggered: TriggeredV3,
      internalMetadata: InternalMetadata): F[Either[ErrorDetails, Unit]] = {

    def asyncOf[A](a: A): F[A] = Async[F].pure(a)

    def issueOrchestratedComm(customerProfile: Option[model.CustomerProfile],
                              channel: Channel,
                              contactProfile: domain.ContactProfile): F[Either[ErrorDetails, Unit]] = {
      channel match {
        case Email =>
          contactProfile.emailAddress
            .map { emailAddr =>
              issueOrchestratedEmail
                .send(customerProfile, emailAddr, triggered)
                .map(_.leftMap(ErrorDetails(_, OrchestrationError)))
            }
            .getOrElse {
              val errorDetails = "Email address missing from customer profile"
              logWarn(triggered.metadata.traceToken, errorDetails)
              asyncOf(Left(ErrorDetails(errorDetails, InvalidProfile)))
            }
        case SMS =>
          contactProfile.mobileNumber
            .map { mobileNumber =>
              issueOrchestratedSMS
                .send(customerProfile, mobileNumber, triggered)
                .map(_.leftMap(ErrorDetails(_, OrchestrationError)))
            }
            .getOrElse {
              logWarn(triggered.metadata.traceToken, "Phone number missing from customer profile")
              asyncOf(Left(ErrorDetails("No valid phone number provided", InvalidProfile)))
            }
        case Print =>
          contactProfile.postalAddress
            .map(ContactAddress.fromCustomerAddress)
            .map { contactAddr =>
              issueOrchestratedPrint
                .send(customerProfile, contactAddr, triggered)
                .map(_.leftMap(ErrorDetails(_, OrchestrationError)))
            }
            .getOrElse {
              logWarn(triggered.metadata.traceToken, "Customer address missing from customer profile")
              asyncOf(Left(ErrorDetails("No valid postal address provided", InvalidProfile)))
            }
        case _ => {
          asyncOf(Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError)))
        }
      }
    }

    def orchestrate(triggeredV3: TriggeredV3,
                    contactProfile: domain.ContactProfile,
                    customerPreferences: Seq[CommunicationPreference],
                    customerProfile: Option[model.CustomerProfile]): F[Either[ErrorDetails, Unit]] = {

      val maybeChannel = channelSelector
        .determineChannel(contactProfile, customerPreferences, triggeredV3)

      maybeChannel match {
        case Left(err)      => asyncOf(Left(err))
        case Right(channel) => issueOrchestratedComm(customerProfile, channel, contactProfile)
      }
    }

    triggered.metadata.deliverTo match {
      case customer @ Customer(_) => {
        val maybeCustomerProfile = getValidatedCustomerProfile(triggered, customer)

        maybeCustomerProfile match {
          case Left(err) => asyncOf(Left(err))
          case Right(customerProfile) =>
            orchestrate(triggered,
                        customerProfile.contactProfile,
                        customerProfile.communicationPreferences,
                        Some(customerProfile.toModel))
        }
      }
      case contactDetails @ ContactDetails(_, _, _) => {
        val maybeContactProfile = getValidatedContactProfile(ContactProfile.fromContactDetails(contactDetails))
        maybeContactProfile match {
          case Left(err)             => asyncOf(Left(err))
          case Right(contactProfile) => orchestrate(triggered, contactProfile, Nil, None)
        }
      }
    }
  }
}
