package com.ovoenergy.orchestration.profile

import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import okhttp3.{HttpUrl, Request, Response}
import com.ovoenergy.orchestration.http.JsonDecoding._
import io.circe.generic.auto._

import scala.util.{Failure, Try}

object CustomerProfiler {

  def apply(httpClient: (Request) => Try[Response], profileApiKey: String, profileHost: String)
           (customerId: String, canary: Boolean): Try[CustomerProfile] = {

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

    httpClient(request).flatMap { response =>
      val responseBody = response.body.string
      if (response.isSuccessful) {
        decodeJson[CustomerProfile](responseBody)
      } else {
        Failure(new Exception(s"Error response (${response.code}) from profile service: $responseBody"))
      }
    }

  }

}
