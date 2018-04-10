package com.ovoenergy.orchestration.processes

import cats.effect.Async
import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import com.ovoenergy.comms.model.{ContactDetails, _}
import com.ovoenergy.orchestration.domain.{
  CommunicationPreference,
  ContactAddress,
  ContactInfo,
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
      issueOrchestratedEmail: IssueOrchestratedComm[EmailAddress, F],
      issueOrchestratedSMS: IssueOrchestratedComm[MobilePhoneNumber, F],
      issueOrchestratedPrint: IssueOrchestratedComm[ContactAddress, F])(
      triggered: TriggeredV3,
      internalMetadata: InternalMetadata): Either[ErrorDetails, F[RecordMetadata]] = {

    def issueOrchestratedComm(customerProfile: Option[model.CustomerProfile],
                              channel: Channel,
                              contactProfile: domain.ContactProfile): Either[ErrorDetails, F[RecordMetadata]] = {
      channel match {
        case Email =>
          contactProfile.emailAddress
            .map(issueOrchestratedEmail.send(customerProfile, _, triggered))
            .toRight {
              val errorDetails = "Email address missing from customer profile"
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
                    customerProfile: Option[model.CustomerProfile]): Either[ErrorDetails, F[RecordMetadata]] = {
      for {
        channel <- channelSelector.determineChannel(contactProfile, customerPreferences, triggeredV3)
        res     <- issueOrchestratedComm(customerProfile, channel, contactProfile)
      } yield res
    }

    triggered.metadata.deliverTo match {
      case customer @ Customer(_) => {
        for {
          customerProfile <- getValidatedCustomerProfile(triggered, customer)
          res <- orchestrate(triggered,
                             customerProfile.contactProfile,
                             customerProfile.communicationPreferences,
                             Some(customerProfile.toModel))
        } yield res

      }
      case contactDetails @ ContactDetails(_, _, _) => {
        for {
          contactProfile <- getValidatedContactProfile(ContactProfile.fromContactDetails(contactDetails))
          res            <- orchestrate(triggered, contactProfile, Nil, None)
        } yield res
      }
    }
  }
}
