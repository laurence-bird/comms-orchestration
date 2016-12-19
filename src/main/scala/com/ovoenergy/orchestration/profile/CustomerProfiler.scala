package com.ovoenergy.orchestration.profile

import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import okhttp3.{Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._

import io.circe.generic.auto._

import scala.util.{Failure, Success, Try}

object CustomerProfiler {

  def apply(canaryEmailAddress: String, httpClient: (Request) => Try[Response], profileApiKey: String, profileHost: String)
           (customerId: String): Try[CustomerProfile] = {

    val successfulStatusCodes = 200 to 299

    def getCustomer: Try[CustomerProfile] = {
      val request = new Request.Builder()
        .url(s"$profileHost/api/customers/$customerId?apikey=$profileApiKey")
        .get()
        .build()

      httpClient(request) match {
        case Success(response) =>
          val responseBody = response.body.string
          if (successfulStatusCodes.contains(response.code)) {
            decodeJson[CustomerProfile](responseBody)
          } else {
            Failure(new Exception(s"Error response (${response.code}) from profile service: $responseBody"))
          }
        case Failure(ex) => Failure(ex)
      }
    }

    customerId match {
      case "canary" =>
        Success(CustomerProfile(
          name = CustomerProfileName(
            title = Some("Master"),
            firstName = "Tweety",
            lastName = "Pie",
            suffix = Some("Canary")
          ),
          emailAddresses = CustomerProfileEmailAddresses(
            primary = Some(canaryEmailAddress),
            secondary = None
          )))
      case _ =>
        getCustomer
    }

  }

}
