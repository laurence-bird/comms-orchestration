package com.ovoenergy.orchestration.profile

import okhttp3.{HttpUrl, Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.domain._
import io.circe.generic.auto._
import cats.syntax.either._
import com.ovoenergy.comms.model.ProfileRetrievalFailed
import com.ovoenergy.comms.serialisation.Retry
import com.ovoenergy.comms.serialisation.Retry.{Failed, RetryConfig}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.util.{Failure, Try}

object CustomerProfiler extends LoggingWithMDC {

  case class CustomerProfileResponse(name: CustomerProfileName,
                                     emailAddress: Option[String],
                                     phoneNumber: Option[String],
                                     communicationPreferences: Seq[CommunicationPreference]) {}

  case class ProfileCustomer(customerId: String, canary: Boolean, traceToken: String)

  def apply(httpClient: (Request) => Try[Response],
            profileApiKey: String,
            profileHost: String,
            retryConfig: RetryConfig)(profileCustomer: ProfileCustomer): Either[ErrorDetails, CustomerProfile] = {

    def toCustomerProfile(response: CustomerProfileResponse) = {
      CustomerProfile(
        response.name,
        response.communicationPreferences,
        ContactProfile(response.emailAddress.map(EmailAddress), response.phoneNumber.map(MobilePhoneNumber), None)
      ) // TODO: Get customer address
    }

    val url = {
      val builder = HttpUrl
        .parse(s"$profileHost/api/customers/${profileCustomer.customerId}")
        .newBuilder()
        .addQueryParameter("apikey", profileApiKey)
      if (profileCustomer.canary) builder.addQueryParameter("canary", "true")
      builder.build()
    }

    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    val result = Retry.retry(
      config = retryConfig,
      onFailure =
        e => logWarn(profileCustomer.traceToken, "Failed to retrieve customer profile from profiles service", e)
    ) { () =>
      httpClient(request).flatMap { response =>
        val responseBody = response.body.string
        if (response.isSuccessful) {
          decodeJson[CustomerProfileResponse](responseBody)
        } else {
          Failure(new Exception(s"Error response (${response.code}) from profile service: $responseBody"))
        }
      }
    }
    result
      .map(r => toCustomerProfile(r.result))
      .leftMap { (err: Failed) =>
        ErrorDetails(s"Failed to retrive customer profile: ${err.finalException.getMessage}", ProfileRetrievalFailed)
      }
  }

}
