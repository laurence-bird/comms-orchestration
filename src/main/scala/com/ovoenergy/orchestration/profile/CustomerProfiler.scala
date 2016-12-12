package com.ovoenergy.orchestration.profile

import scala.concurrent.Future

object CustomerProfiler {

  case class CustomerProfileName(title: String, firstName: String, lastName: String, suffix: String)
  case class CustomerProfileEmailAddresses(primary: String, secondary: String)
  case class CustomerProfile(name: CustomerProfileName, emailAddresses: CustomerProfileEmailAddresses)

  def apply(customerId: String): Future[CustomerProfile] = {
    Future.successful(CustomerProfile(
      name = CustomerProfileName(
        title = "Mr",
        firstName = "John",
        lastName = "Smith",
        suffix = "Mr"
      ),
      emailAddresses = CustomerProfileEmailAddresses(
        primary = "some.email@ovoenergy.com",
        secondary = ""
      )))
  }

}
