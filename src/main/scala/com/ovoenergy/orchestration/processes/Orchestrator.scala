package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{
  ContactProfile,
  CustomerDeliveryDetails,
  CustomerProfile,
  CustomerProfileName
}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.either._
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.processes.Scheduler.CustomerId

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)
  type SendEvent = (CustomerDeliveryDetails, TriggeredV3, InternalMetadata) => Future[RecordMetadata]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            determineChannel: (ContactProfile, TriggeredV3) => Either[ErrorDetails, Channel],
            validateProfile: (CustomerProfile) => Either[ErrorDetails, CustomerProfile],
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

    def buildProfileForChannel(contactProfile: ContactProfile,
                               customerProfileName: Option[CustomerProfileName],
                               channel: Channel): Either[ErrorDetails, CustomerDeliveryDetails] = {
      channel match {
        case SMS => {
          contactProfile.phoneNumber
            .map(p => CustomerDeliveryDetails(customerProfileName, p))
            .toRight {
              logWarn(triggered.metadata.traceToken, "Phone number missing from customer profile")
              ErrorDetails("Phone number missing from customer profile", InvalidProfile)
            }
        }
        case Email =>
          contactProfile.emailAddress
            .map(e => CustomerDeliveryDetails(customerProfileName, e))
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
                    contactProfile: ContactProfile,
                    customerProfileName: Option[CustomerProfileName]) = {
      for {
        channel     <- determineChannel(contactProfile, triggeredV3)
        eventSender <- selectEventSender(channel)
        customerDeliverydetails: CustomerDeliveryDetails <- buildProfileForChannel(contactProfile,
                                                                                   customerProfileName,
                                                                                   channel)
      } yield eventSender(customerDeliverydetails, triggered, internalMetadata)
    }

    def retrieveCustomerProfile(customerId: String, triggeredV3: TriggeredV3) = {
      for {
        customerProfile: CustomerProfile <- profileCustomer(customerId,
                                                            triggered.metadata.canary,
                                                            triggered.metadata.traceToken)
        validatedProfile <- validateProfile(customerProfile)
      } yield {
        validatedProfile
      }
    }

    triggered.metadata.deliverTo match {
      case Customer(customerId) => {
        for {
          customerProfile <- retrieveCustomerProfile(customerId, triggered)
          res             <- orchestrate(triggered, customerProfile.contactProfile, Some(customerProfile.name))
        } yield res
      }
      case ContactDetails(emailAddr, phoneNo) =>
        val contactProfile = ContactProfile(emailAddr, phoneNo, Seq.empty)
        orchestrate(triggered, contactProfile, None)
    }
  }
}
