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
  type Orchestrator = (CustomerDeliveryDetails, TriggeredV2, InternalMetadata) => Future[RecordMetadata]

  def apply(profileCustomer: (String, Boolean, String) => Either[ErrorDetails, CustomerProfile],
            determineChannel: (CustomerProfile, TriggeredV2) => Either[ErrorDetails, Channel],
            validateProfile: (CustomerProfile) => Either[ErrorDetails, CustomerProfile],
            orchestrateEmail: Orchestrator,
            orchestrateSMS: Orchestrator)(
      triggered: TriggeredV2,
      internalMetadata: InternalMetadata): Either[ErrorDetails, Future[RecordMetadata]] = {

    def selectOrchestratorforChannel(channel: Channel): Either[ErrorDetails, Orchestrator] =
      channel match {
        case Email => Right(orchestrateEmail)
        case SMS   => Right(orchestrateSMS)
        case _     => Left(ErrorDetails(s"Unsupported channel selected $channel", OrchestrationError))
      }

    for {
      customerProfile <- profileCustomer(triggered.metadata.customerId,
                                         triggered.metadata.canary,
                                         triggered.metadata.traceToken)
      validatedProfile        <- validateProfile(customerProfile)
      channel                 <- determineChannel(validatedProfile, triggered)
      orchestrateComm         <- selectOrchestratorforChannel(channel)
      customerDeliveryDetails <- buildProfileForChannel(validatedProfile, channel)
    } yield orchestrateComm(customerDeliveryDetails, triggered, internalMetadata)
  }

  private def buildProfileForChannel(customerProfile: CustomerProfile,
                                     channel: Channel): Either[ErrorDetails, CustomerDeliveryDetails] = {
    channel match {
      case SMS => {
        customerProfile.phoneNumber
          .map(p => CustomerDeliveryDetails(customerProfile.name, p))
          .toRight(ErrorDetails("Phone number missing from customer profile", InvalidProfile))
      }
      case Email =>
        customerProfile.emailAddress
          .map(e => CustomerDeliveryDetails(customerProfile.name, e))
          .toRight(ErrorDetails("Email address missing from customer profile", InvalidProfile))

      case otherChannel =>
        Left(ErrorDetails(s"Channel ${otherChannel.toString} unavailable", ErrorCode.OrchestrationError))
    }

  }
}
