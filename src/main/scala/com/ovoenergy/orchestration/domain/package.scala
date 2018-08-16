package com.ovoenergy.orchestration

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
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

}
