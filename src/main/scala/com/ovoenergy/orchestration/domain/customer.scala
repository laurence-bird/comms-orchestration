package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{Channel, CommType}
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
package object customer {

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])
  case class CustomerProfileEmailAddresses(primary: Option[String], secondary: Option[String])
  case class CommunicationPreference(commType: CommType, channels: Seq[Channel])

  implicit val commTypeDecoder = deriveEnumerationDecoder[CommType]
  implicit val channelDecoder  = deriveEnumerationDecoder[Channel]

  case class CustomerProfile(name: CustomerProfileName, contactProfile: ContactProfile) {
    def toModel = model.CustomerProfile(
      firstName = name.firstName,
      lastName = name.lastName
    )
  }

  case class CustomerProfileResponse(name: CustomerProfileName,
                                     emailAddress: Option[String],
                                     phoneNumber: Option[String],
                                     communicationPreferences: Seq[CommunicationPreference]) {
    def toCustomerProfile = CustomerProfile(name, ContactProfile(emailAddress, phoneNumber, communicationPreferences))
  }

  case class ContactProfile(emailAddress: Option[String],
                            phoneNumber: Option[String],
                            communicationPreferences: Seq[CommunicationPreference])

}
