package com.ovoenergy.orchestration.profile

import scala.util.{Failure, Success, Try}

object CustomerProfiler {

  case class CustomerProfileName(title: String, firstName: String, lastName: String, suffix: String)
  case class CustomerProfileEmailAddresses(primary: String, secondary: String)
  case class CustomerProfile(name: CustomerProfileName, emailAddresses: CustomerProfileEmailAddresses)

  //TODO - Call new Customer Profile service
  def apply(customerId: String): Try[CustomerProfile] = {
    customerId match {
      case "invalidCustomer" =>
        Success(CustomerProfile(
          name = CustomerProfileName(
            title = "",
            firstName = "",
            lastName = "",
            suffix = ""
          ),
          emailAddresses = CustomerProfileEmailAddresses(
            primary = "",
            secondary = ""
          )))
      case "errorCustomer" =>
        Failure(new Exception("Some failure reason"))
      case _ =>
        Success(CustomerProfile(
          name = CustomerProfileName(
            title = "Mr",
            firstName = "John",
            lastName = "Smith",
            suffix = ""
          ),
          emailAddresses = CustomerProfileEmailAddresses(
            primary = "some.email@ovoenergy.com",
            secondary = ""
          )))
    }

  }

}
