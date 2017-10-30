package com.ovoenergy.orchestration

import java.time.{Instant, OffsetDateTime}

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
package object domain {

  private val validUkMobileRegex = """^\+447\d{9}$""".r
  private val phoneNumberPrefix  = """^(07|447|0{1,2}447)"""

  implicit val commTypeDecoder = deriveEnumerationDecoder[CommType]
  implicit val channelDecoder  = deriveEnumerationDecoder[Channel]

  sealed trait ContactInfo

  object MobilePhoneNumber {
    def create(number: String): Either[String, MobilePhoneNumber] = {
      val strippedPhoneNumber = number.trim
        .replaceAll("[^0-9]", "")
        .replaceFirst(phoneNumberPrefix, """+447""")
      validUkMobileRegex
        .findFirstIn(strippedPhoneNumber)
        .map(MobilePhoneNumber(_))
        .toRight("Invalid phone number provided")
    }
  }
  case class MobilePhoneNumber private (number: String) extends ContactInfo

  case class EmailAddress(address: String) extends ContactInfo

  case class ContactAddress(line1: String,
                            line2: String,
                            town: String,
                            county: String,
                            postcode: String,
                            country: String) extends ContactInfo

  object ContactAddress{
    def fromCustomerAddress(customerAddress: CustomerAddress) = {
      ContactAddress(customerAddress.line1, customerAddress.line2, customerAddress.town, customerAddress.county, customerAddress.postcode, customerAddress.country)
    }
  }

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])

  case class CustomerProfileEmailAddresses(primary: Option[EmailAddress], secondary: Option[EmailAddress])

  case class CommunicationPreference(commType: CommType, channels: Seq[Channel])

  case class ContactProfile(emailAddress: Option[EmailAddress], mobileNumber: Option[MobilePhoneNumber], postalAddress: Option[CustomerAddress])

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

  def triggeredV2ToV3(triggeredV2: TriggeredV2): TriggeredV3 = {
    def metadataToV2(metadata: Metadata): MetadataV2 = {
      MetadataV2(
        createdAt = OffsetDateTime.parse(metadata.createdAt).toInstant,
        eventId = metadata.eventId,
        traceToken = metadata.traceToken,
        commManifest = metadata.commManifest,
        deliverTo = Customer(metadata.customerId),
        friendlyDescription = metadata.friendlyDescription,
        source = metadata.source,
        canary = metadata.canary,
        sourceMetadata = metadata.sourceMetadata.map(metadataToV2),
        triggerSource = metadata.triggerSource
      )
    }

    TriggeredV3(
      metadata = metadataToV2(triggeredV2.metadata),
      templateData = triggeredV2.templateData,
      deliverAt = triggeredV2.deliverAt.map(OffsetDateTime.parse(_).toInstant),
      expireAt = triggeredV2.expireAt.map(OffsetDateTime.parse(_).toInstant),
      preferredChannels = triggeredV2.preferredChannels
    )
  }

}
