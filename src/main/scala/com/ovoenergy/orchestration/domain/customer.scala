package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{Channel, CommType}
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
package object customer {

  implicit val commTypeDecoder = deriveEnumerationDecoder[CommType]
  implicit val channelDecoder  = deriveEnumerationDecoder[Channel]

  sealed trait ContactInfo
  case class MobilePhoneNumber(number: String) extends ContactInfo
  case class EmailAddress(address: String)     extends ContactInfo

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])

  case class CustomerProfileEmailAddresses(primary: Option[EmailAddress], secondary: Option[EmailAddress])

  case class CommunicationPreference(commType: CommType, channels: Seq[Channel])

  case class ContactProfile(emailAddress: Option[EmailAddress], mobileNumber: Option[MobilePhoneNumber])

  case class CustomerProfile(name: CustomerProfileName,
                             communicationPreferences: Seq[CommunicationPreference],
                             contactProfile: ContactProfile) {
    def toModel = model.CustomerProfile(
      firstName = name.firstName,
      lastName = name.lastName
    )
  }

}
