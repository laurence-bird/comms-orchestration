package com.ovoenergy.orchestration

import java.time.{Instant, OffsetDateTime}

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
package object domain {

  implicit val commTypeDecoder = deriveEnumerationDecoder[CommType]
  implicit val channelDecoder  = deriveEnumerationDecoder[Channel]

  sealed trait ContactInfo

  case class MobilePhoneNumber private (number: String) extends ContactInfo

  case class EmailAddress(address: String) extends ContactInfo

  case class ContactAddress(line1: String,
                            line2: Option[String],
                            town: String,
                            county: Option[String],
                            postcode: String,
                            country: Option[String])
      extends ContactInfo

  object ContactAddress {
    def fromCustomerAddress(customerAddress: CustomerAddress) = {
      ContactAddress(
        customerAddress.line1,
        customerAddress.line2,
        customerAddress.town,
        customerAddress.county,
        customerAddress.postcode,
        customerAddress.country
      )
    }
  }

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])

  case class CustomerProfileEmailAddresses(primary: Option[EmailAddress], secondary: Option[EmailAddress])

  case class CommunicationPreference(commType: CommType, channels: Seq[Channel])

  case class ContactProfile(emailAddress: Option[EmailAddress],
                            mobileNumber: Option[MobilePhoneNumber],
                            postalAddress: Option[CustomerAddress])

  object ContactProfile {
    def fromContactDetails(contactDetails: ContactDetails) =
      ContactProfile(contactDetails.emailAddress.map(EmailAddress),
                     contactDetails.phoneNumber.map(MobilePhoneNumber),
                     contactDetails.postalAddress)
  }

  case class CustomerProfile(name: CustomerProfileName,
                             communicationPreferences: Seq[CommunicationPreference],
                             contactProfile: ContactProfile) {
    def toModel = model.CustomerProfile(
      firstName = name.firstName,
      lastName = name.lastName
    )
  }

  def cancellationRequestedToV2(cancellationRequested: CancellationRequested): CancellationRequestedV2 = {
    CancellationRequestedV2(
      metadata = GenericMetadataV2(
        createdAt = Instant.parse(cancellationRequested.metadata.createdAt),
        eventId = cancellationRequested.metadata.eventId,
        traceToken = cancellationRequested.metadata.traceToken,
        source = cancellationRequested.metadata.source,
        canary = cancellationRequested.metadata.canary
      ),
      commName = cancellationRequested.commName,
      customerId = cancellationRequested.customerId
    )
  }

  case class FailureDetails(deliverTo: DeliverTo,
                            commId: CommId,
                            traceToken: TraceToken,
                            eventId: EventId,
                            reason: String,
                            errorCode: ErrorCode,
                            failureType: FailureType)

  object FailureDetails {
    def apply(deliverTo: DeliverTo,
              commId: String,
              traceToken: String,
              eventId: String,
              reason: String,
              errorCode: ErrorCode,
              failureType: FailureType): FailureDetails = {
      FailureDetails(
        deliverTo,
        CommId(commId),
        TraceToken(traceToken),
        EventId(eventId),
        reason,
        errorCode,
        failureType
      )
    }
  }

  case class CommId(value: String)
  case class EventId(value: String)
  case class TraceToken(value: String)

  sealed trait FailureType

  case object CancellationFailure extends FailureType
  case object InternalFailure     extends FailureType
}
