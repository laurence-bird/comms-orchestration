package com.ovoenergy.orchestration.processes

import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.{InvalidProfile, OrchestrationError}
import com.ovoenergy.comms.model.{InternalMetadata, _}
import com.ovoenergy.orchestration.domain.customer.{CustomerDeliveryDetails, CustomerProfile}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.either._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Orchestrator extends LoggingWithMDC {
  case class ErrorDetails(reason: String, errorCode: ErrorCode)
  type SendEvent = (CustomerDeliveryDetails, TriggeredV2, InternalMetadata) => Future[RecordMetadata]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            determineChannel: (CustomerProfile, TriggeredV2) => Either[ErrorDetails, Channel],
            validateProfile: (CustomerProfile) => Either[ErrorDetails, CustomerProfile],
            sendOrchestratedEmailEvent: SendEvent,
            sendOrchestratedSMSEvent: SendEvent)(
      triggered: TriggeredV2,
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
          customerProfile.mobileNumber
            .map(p => CustomerDeliveryDetails(customerProfile.name, p))
            .toRight{
              logWarn(triggered.metadata.traceToken , "Phone number missing from customer profile")
              ErrorDetails("Phone number missing from customer profile", InvalidProfile)
            }
        }
        case Email =>
          customerProfile.emailAddress
            .map(e => CustomerDeliveryDetails(customerProfile.name, e))
            .toRight{
              val errorDetails = "Phone number missing from customer profile"
              logWarn(triggered.metadata.traceToken , errorDetails)
              ErrorDetails(errorDetails, InvalidProfile)
            }

        case otherChannel =>{
          val errorDetails = s"Channel ${otherChannel.toString} unavailable"
          logWarn(triggered.metadata.traceToken , errorDetails)
          Left(ErrorDetails(errorDetails, ErrorCode.OrchestrationError))
        }
      }

    }

    for {
      customerProfile <- profileCustomer(triggered.metadata.customerId,
                                         triggered.metadata.canary,
                                         triggered.metadata.traceToken)
      validatedProfile        <- validateProfile(customerProfile)
      channel                 <- determineChannel(validatedProfile, triggered)
      eventSender             <- selectEventSender(channel)
      customerDeliveryDetails <- buildProfileForChannel(validatedProfile, channel)
    } yield eventSender(customerDeliveryDetails, triggered, internalMetadata)
  }
}
