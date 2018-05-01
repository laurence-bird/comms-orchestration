package com.ovoenergy.orchestration.processes

import cats.data.EitherT
import cats.effect.{Async, IO}
import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
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
import cats.implicits._

import scala.concurrent.ExecutionContext

object Orchestrator extends LoggingWithMDC {

  case class ErrorDetails(reason: String, errorCode: ErrorCode)

  object ErrorDetails {
    implicit val loggableErrorDetails = Loggable.instance[ErrorDetails] { ed =>
      Map(
        "errorCode"   -> ed.errorCode.toString,
        "errorReason" -> ed.reason
      )
    }
  }

  def apply[F[_]: Async](channelSelector: ChannelSelector,
                         getValidatedCustomerProfile: (TriggeredV3, Customer) => F[Either[
                           ErrorDetails,
                           domain.CustomerProfile]], // TODO: Handle this throwing naughty exceptions
                         getValidatedContactProfile: ContactProfile => Either[ErrorDetails, ContactProfile],
                         issueOrchestratedEmail: IssueOrchestratedComm[EmailAddress, F],
                         issueOrchestratedSMS: IssueOrchestratedComm[MobilePhoneNumber, F],
                         issueOrchestratedPrint: IssueOrchestratedComm[ContactAddress, F])(
      triggered: TriggeredV3,
      internalMetadata: InternalMetadata)(implicit ec: ExecutionContext): F[Either[ErrorDetails, RecordMetadata]] = {

    def issueOrchestratedComm(customerProfile: Option[model.CustomerProfile],
                              channel: Channel,
                              contactProfile: domain.ContactProfile): Either[ErrorDetails, F[RecordMetadata]] = {
      channel match {
        case Email =>
          contactProfile.emailAddress
            .map(issueOrchestratedEmail.send(customerProfile, _, triggered))
            .toRight {
              val errorDetails = "Email address missing from customer profile"
              warn(triggered)(errorDetails)
              ErrorDetails(errorDetails, InvalidProfile)
            }
        case SMS =>
          contactProfile.mobileNumber
            .map(issueOrchestratedSMS.send(customerProfile, _, triggered))
            .toRight {
              warn(triggered)("Phone number missing from customer profile")
              ErrorDetails("No valid phone number provided", InvalidProfile)
            }

        case Print =>
          contactProfile.postalAddress
            .map(ContactAddress.fromCustomerAddress)
            .map(issueOrchestratedPrint.send(customerProfile, _, triggered))
            .toRight {
              warn(triggered)("Customer address missing from customer profile")
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

    val result: EitherT[F, ErrorDetails, RecordMetadata] = triggered.metadata.deliverTo match {
      case customer @ Customer(_) => {
        for {
          customerProfile <- EitherT(getValidatedCustomerProfile(triggered, customer))
          orchestrationResult <- {
            val res = orchestrate(triggered,
                                  customerProfile.contactProfile,
                                  customerProfile.communicationPreferences,
                                  Some(customerProfile.toModel))

            EitherT(res.traverse(identity))
          }
        } yield orchestrationResult
      }
      case contactDetails @ ContactDetails(_, _, _) => {
        for {
          contactProfile <- EitherT(
            Async[F].delay(getValidatedContactProfile(ContactProfile.fromContactDetails(contactDetails))))
          orchestrationResult <- {
            val res = orchestrate(triggered, contactProfile, Nil, None)
            EitherT(res.traverse(identity))
          }
        } yield orchestrationResult
      }
    }

    result.value
  }
}
