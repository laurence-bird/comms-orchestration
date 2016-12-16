package com.ovoenergy.orchestration.domain

package object customerProfile {

  case class CustomerProfileName(title: Option[String], firstName: String, lastName: String, suffix: Option[String])
  case class CustomerProfileEmailAddresses(primary: Option[String], secondary: Option[String])
  case class CustomerProfile(name: CustomerProfileName, emailAddresses: CustomerProfileEmailAddresses)

}
