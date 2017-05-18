package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model
import com.ovoenergy.orchestration.domain.{customer => domain}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.either._
import com.ovoenergy.comms.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)
  type SendEvent = (Option[CustomerProfile], String, TriggeredV3, InternalMetadata) => Future[RecordMetadata]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, domain.CustomerProfile],
            determineChannel: (domain.ContactProfile, TriggeredV3) => Either[ErrorDetails, Channel],
            validateProfile: (domain.CustomerProfile) => Either[ErrorDetails, domain.CustomerProfile],
            sendOrchestratedEmailEvent: SendEvent,
            sendOrchestratedSMSEvent: SendEvent)(
      triggered: TriggeredV3,
      internalMetadata: InternalMetadata): Either[ErrorDetails, Future[RecordMetadata]] = {

    def selectEventSender(channel: Channel): Either[ErrorDetails, SendEvent] =
      channel match {
        case Email => Right(sendOrchestratedEmailEvent)
        case SMS   => Right(sendOrchestratedSMSEvent)
        case _     => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }

    def determineDeliverTo(contactProfile: domain.ContactProfile, channel: Channel): Either[ErrorDetails, String] = {
      channel match {
        case SMS => {
          contactProfile.phoneNumber
            .toRight {
              logWarn(triggered.metadata.traceToken, "Phone number missing from customer profile")
              ErrorDetails("Phone number missing from customer profile", InvalidProfile)
            }
        }
        case Email =>
          contactProfile.emailAddress
            .toRight {
              val errorDetails = "Phone number missing from customer profile"
              logWarn(triggered.metadata.traceToken, errorDetails)
              ErrorDetails(errorDetails, InvalidProfile)
            }

        case otherChannel => {
          val errorDetails = s"Channel ${otherChannel.toString} unavailable"
          logWarn(triggered.metadata.traceToken, errorDetails)
          Left(ErrorDetails(errorDetails, OrchestrationError))
        }
      }
    }

    def orchestrate(triggeredV3: TriggeredV3,
                    contactProfile: domain.ContactProfile,
                    customerProfile: Option[model.CustomerProfile]) = {
      for {
        channel     <- determineChannel(contactProfile, triggeredV3)
        eventSender <- selectEventSender(channel)
        deliverTo   <- determineDeliverTo(contactProfile, channel)
      } yield eventSender(customerProfile, deliverTo, triggered, internalMetadata)
    }

    def retrieveCustomerProfile(customerId: String,
                                triggeredV3: TriggeredV3): Either[ErrorDetails, domain.CustomerProfile] = {
      for {
        customerProfile  <- profileCustomer(customerId, triggered.metadata.canary, triggered.metadata.traceToken)
        validatedProfile <- validateProfile(customerProfile)
      } yield {
        validatedProfile
      }
    }

    triggered.metadata.deliverTo match {
      case Customer(customerId) => {
        for {
          customerProfile <- retrieveCustomerProfile(customerId, triggered)
          res             <- orchestrate(triggered, customerProfile.contactProfile, Some(customerProfile.toModel))
        } yield res
      }
      case ContactDetails(emailAddr, phoneNo) =>
        val contactProfile = domain.ContactProfile(emailAddr, phoneNo, Seq.empty)
        orchestrate(triggered, contactProfile, None)
    }
  }
}
