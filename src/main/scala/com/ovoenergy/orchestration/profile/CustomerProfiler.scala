package com.ovoenergy.orchestration.profile

import okhttp3.{HttpUrl, Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.domain._
import io.circe.generic.auto._
import cats.syntax.either._
import cats.implicits._
import com.ovoenergy.comms.model.ProfileRetrievalFailed
import com.ovoenergy.orchestration.http.Retry
import com.ovoenergy.orchestration.http.Retry.RetryConfig
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.util.{Failure, Try}

object CustomerProfiler extends LoggingWithMDC {

  case class CustomerProfileResponse(name: CustomerProfileName,
                                     emailAddress: Option[String],
                                     phoneNumber: Option[String],
                                     communicationPreferences: Seq[CommunicationPreference])

  private val clientErrorRange = 400 to 499
  case class ClientError(message: String)          extends Exception(message)
  case class DeserialisationError(message: String) extends Exception(message)

  def apply(httpClient: (Request) => Try[Response],
            profileApiKey: String,
            profileHost: String,
            retryConfig: RetryConfig)(customerId: String,
                                      canary: Boolean,
                                      traceToken: String): Either[ErrorDetails, CustomerProfile] = {

    def toCustomerProfile(response: CustomerProfileResponse) = {
      val validNumber = response.phoneNumber.map(MobilePhoneNumber.create) match {
        case Some(Right(number)) => Some(number)
        case Some(Left(e)) =>
          logInfo(traceToken, s"Invalid phone number returned for customer $customerId")
          None
        case _ => None

      }
      CustomerProfile(response.name,
                      response.communicationPreferences,
                      ContactProfile(response.emailAddress.map(EmailAddress), validNumber))
    }

    val url = {
      val builder = HttpUrl
        .parse(s"$profileHost/api/customers/$customerId")
        .newBuilder()
        .addQueryParameter("apikey", profileApiKey)
      if (canary) builder.addQueryParameter("canary", "true")
      builder.build()
    }

    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    val processFailure = (e: Throwable) => {
      e match {
        case ClientError(message) =>
          logWarn(traceToken, message)
          true // do not retry
        case DeserialisationError(message) =>
          logError(traceToken, message)
          true // do not retry
        case e: Throwable =>
          logWarn(traceToken, "Failed to retrieve customer profile from profiles service", e)
          false
      }
    }

    val result = Retry.retry(
      config = retryConfig,
      shouldStop = processFailure
    ) { () =>
      httpClient(request).flatMap { (response: Response) =>
        val responseBody = response.body.string
        val responseCode = response.code()
        if (response.isSuccessful) {
          decodeJson[CustomerProfileResponse](responseBody)
        } else if (clientErrorRange contains responseCode) {
          Failure(ClientError(s"Error response ($responseCode) from profile service: $responseBody"))
        } else {
          Failure(new Exception(s"Error response ($responseCode) from profile service: $responseBody"))
        }
      }
    }
    result
      .map(r => toCustomerProfile(r.result))
      .leftMap { (err: Retry.Failed) =>
        println(err)
        ErrorDetails(
          s"Failed to retrieve customer profile after ${err.attemptsMade} attempt(s): ${err.finalException.getMessage}",
          ProfileRetrievalFailed)
      }
  }
}
