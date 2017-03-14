package com.ovoenergy.orchestration.profile

import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import okhttp3.{HttpUrl, Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.retry.Retry.{Failed, RetryConfig}
import io.circe.generic.auto._
import cats.syntax.either._
import com.ovoenergy.comms.model.ErrorCode.ProfileRetrievalFailed
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.retry.Retry

import scala.util.{Failure, Try}

object CustomerProfiler extends LoggingWithMDC {

  def apply(httpClient: (Request) => Try[Response],
            profileApiKey: String,
            profileHost: String,
            retryConfig: RetryConfig)(customerId: String,
                                      canary: Boolean,
                                      traceToken: String): Either[ErrorDetails, CustomerProfile] = {

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

    val result = Retry.retry(
      config = retryConfig,
      onFailure = e => logWarn(traceToken, "Failed to retrieve customer profile from profiles service", e)
    ) { () =>
      httpClient(request).flatMap { response =>
        val responseBody = response.body.string
        if (response.isSuccessful) {
          decodeJson[CustomerProfile](responseBody)
        } else {
          Failure(new Exception(s"Error response (${response.code}) from profile service: $responseBody"))
        }
      }
    }
    result
      .map(_.result)
      .leftMap { (err: Failed) =>
        ErrorDetails(s"Failed to retrive customer profile: ${err.finalException.getMessage}", ProfileRetrievalFailed)
      }
  }

}
