package com.ovoenergy.orchestration.profile

import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import okhttp3.{HttpUrl, Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.profile.Retry.RetryConfig
import io.circe.generic.auto._
import cats.syntax.either._

import scala.util.{Failure, Try}

object CustomerProfiler extends LoggingWithMDC {

  val loggerName = "CustomerProfiler"

  def apply(httpClient: (Request) => Try[Response], profileApiKey: String, profileHost: String, retryConfig: RetryConfig)
           (customerId: String, canary: Boolean, traceToken: String): Try[CustomerProfile] = {

    val url = {
      val builder = HttpUrl.parse(s"$profileHost/api/customers/$customerId").newBuilder()
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
    ){ () =>
      httpClient(request).flatMap { response =>
        val responseBody = response.body.string
        if (response.isSuccessful) {
          decodeJson[CustomerProfile](responseBody)
        } else {
          Failure(new Exception(s"Error response (${response.code}) from profile service: $responseBody"))
        }
      }
    }
    result.map(_.result).toTry
  }

}
