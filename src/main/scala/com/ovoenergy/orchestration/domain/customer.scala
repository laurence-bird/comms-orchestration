package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model.{Channel, CommType}
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
package object customer {

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])
  case class CustomerProfileEmailAddresses(primary: Option[String], secondary: Option[String])
  case class CommunicationPreference(commType: CommType, channels: Seq[Channel])

  implicit val commTypeDecoder = deriveEnumerationDecoder[CommType]
  implicit val channelDecoder  = deriveEnumerationDecoder[Channel]

  case class CustomerProfile(name: CustomerProfileName,
                             emailAddress: Option[String],
                             mobileNumber: Option[String],
                             communicationPreferences: Seq[CommunicationPreference])

  // DeliverTo is either phone number or email address
  case class CustomerDeliveryDetails(name: CustomerProfileName, deliverTo: String)
  
}
