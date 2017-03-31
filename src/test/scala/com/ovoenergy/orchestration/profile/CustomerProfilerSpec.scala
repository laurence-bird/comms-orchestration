package com.ovoenergy.orchestration.profile

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.CommType.Service
import com.ovoenergy.orchestration.domain.customer.CommunicationPreference._
import com.ovoenergy.comms.model.ErrorCode.ProfileRetrievalFailed
import com.ovoenergy.orchestration.domain.customer
import com.ovoenergy.orchestration.domain.customer.{CustomerProfile, CustomerProfileName}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.retry.Retry
import okhttp3._
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.util.{Failure, Success}

class CustomerProfilerSpec extends FlatSpec with Matchers with EitherValues {

  val failureHttpClient = (request: Request) => Failure(new Exception("uh oh"))
  val profileApiKey     = "apiKey"
  val profileHost       = "http://somehost.com"
  val traceToken        = "token"
  val retryConfig       = Retry.RetryConfig(1, Retry.Backoff.retryImmediately)

  val validResponseJson =
    new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")), StandardCharsets.UTF_8)

  behavior of "Customer Profiler"

  it should "Fail when request fails" in {
    val result =
      CustomerProfiler(failureHttpClient, profileApiKey, profileHost, retryConfig)("whatever",
                                                                                   canary = false,
                                                                                   traceToken)

    result shouldBe Left(ErrorDetails(s"Failed to retrive customer profile: uh oh", ProfileRetrievalFailed))
  }

  it should "Fail when response is not a success code" in {
    val nonOkResponseHttpClient = (request: Request) =>
      Success(
        new Response.Builder()
          .protocol(Protocol.HTTP_1_1)
          .request(request)
          .code(401)
          .body(ResponseBody.create(MediaType.parse("UTF-8"), "Some error message"))
          .build())

    val result = CustomerProfiler(nonOkResponseHttpClient, profileApiKey, profileHost, retryConfig)("whatever",
                                                                                                    canary = false,
                                                                                                    traceToken)
    result shouldBe Left(
      ErrorDetails("Failed to retrive customer profile: Error response (401) from profile service: Some error message",
                   ProfileRetrievalFailed))
  }

  it should "Fail when response body is invalid" in {
    val badResponseHttpClient = (request: Request) =>
      Success(
        new Response.Builder()
          .protocol(Protocol.HTTP_1_1)
          .request(request)
          .code(200)
          .body(ResponseBody.create(MediaType.parse("UTF-8"), "{\"some\":\"value\"}"))
          .build())

    val result = CustomerProfiler(badResponseHttpClient, profileApiKey, profileHost, retryConfig)("whatever",
                                                                                                  canary = false,
                                                                                                  traceToken)
    result.isLeft shouldBe true
    result.left.value.errorCode shouldBe ProfileRetrievalFailed
    result.left.value.reason should include("Failed to retrive customer profile: Invalid JSON")
  }

  it should "Succeed when response is valid" in {
    val okResponseHttpClient = (request: Request) => {
      request.url.toString shouldBe s"$profileHost/api/customers/whatever?apikey=$profileApiKey"
      request.method shouldBe "GET"

      Success(
        new Response.Builder()
          .protocol(Protocol.HTTP_1_1)
          .request(request)
          .code(200)
          .body(ResponseBody.create(MediaType.parse("UTF-8"), validResponseJson))
          .build())
    }

    val result = CustomerProfiler(okResponseHttpClient, profileApiKey, profileHost, retryConfig)("whatever",
                                                                                                 canary = false,
                                                                                                 traceToken)
    result shouldBe Right(
      CustomerProfile(
        name = CustomerProfileName(
          title = Some("Mr"),
          firstName = "John",
          lastName = "Wayne",
          suffix = None
        ),
        emailAddress = Some("qatesting@ovoenergy.com"),
        mobileNumber = Some("+447985631544"),
        communicationPreferences = Seq(
          customer.CommunicationPreference(
            Service,
            Seq(SMS, Email)
          )
        )
      ))
  }

  it should "Ask the profiles service for the canary when requested" in {
    val httpClient = (request: Request) => {
      request.url.queryParameter("canary") shouldBe "true"
      request.method shouldBe "GET"

      Success(
        new Response.Builder()
          .protocol(Protocol.HTTP_1_1)
          .request(request)
          .code(200)
          .body(ResponseBody.create(MediaType.parse("UTF-8"), validResponseJson))
          .build())
    }
    val result =
      CustomerProfiler(httpClient, profileApiKey, profileHost, retryConfig)("whatever", canary = true, traceToken)
    result match {
      case Right(customerProfile) =>
      // ok
      case Left(err) =>
        fail(s"Unexpected failure: ${err.reason}")
    }
  }

}
