package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.customer.{CustomerDeliveryDetails, CustomerProfile}
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
            determineChannel: (CustomerProfile, TriggeredV3) => Either[ErrorDetails, Channel],
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

    def buildProfileForChannel(customerProfile: CustomerProfile,
                               channel: Channel): Either[ErrorDetails, CustomerDeliveryDetails] = {
      channel match {
        case SMS => {
          customerProfile.phoneNumber
            .map(p => CustomerDeliveryDetails(customerProfile.name, p))
            .toRight {
              logWarn(triggered.metadata.traceToken, "Phone number missing from customer profile")
              ErrorDetails("Phone number missing from customer profile", InvalidProfile)
            }
        }
        case Email =>
          customerProfile.emailAddress
            .map(e => CustomerDeliveryDetails(customerProfile.name, e))
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

    def orchestrateForCustomer(triggeredV3: TriggeredV3, customerId: String) = {
      for {
        customerProfile         <- profileCustomer(customerId, triggered.metadata.canary, triggered.metadata.traceToken)
        validatedProfile        <- validateProfile(customerProfile)
        channel                 <- determineChannel(validatedProfile, triggered)
        eventSender             <- selectEventSender(channel)
        customerDeliveryDetails <- buildProfileForChannel(validatedProfile, channel)
      } yield eventSender(customerDeliveryDetails, triggered, internalMetadata)
    }

    triggered.metadata.deliverTo match {
      case Customer(customerId)               => orchestrateForCustomer(triggered, customerId)
      case ContactDetails(emailAddr, phoneNo) => Left(ErrorDetails("Not implemented", InvalidProfile))
    }
  }
}
