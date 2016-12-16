package com.ovoenergy.orchestration.profile

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.ovoenergy.orchestration.domain.customerProfile.{CustomerProfile, CustomerProfileEmailAddresses, CustomerProfileName}
import okhttp3._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class CustomerProfilerSpec extends FlatSpec
  with Matchers {

  val canaryEmailAddress = "canary@email.com"

  val failureHttpClient = (request: Request) => Failure(new Exception())
  val profileApiKey = "apiKey"
  val profileHost = "http://somehost.com"

  behavior of "Customer Profiler"

  it should "Return canary when requested" in {
    val result = CustomerProfiler(canaryEmailAddress, failureHttpClient, profileApiKey, profileHost)("canary")
    result match {
      case Success(customerProfile) =>
        customerProfile.emailAddresses.primary shouldBe Some(canaryEmailAddress)
      case Failure(ex) =>
        fail(ex)
    }
  }

  it should "Fail when request fails" in {
    val result = CustomerProfiler(canaryEmailAddress, failureHttpClient, profileApiKey, profileHost)("whatever")
    result match {
      case Success(customerProfile) =>
        fail("Failure expected")
      case Failure(ex) =>
      //OK
    }
  }

  it should "Fail when response is not a success code" in {
    val nonOkResponseHttpClient = (request: Request) =>
      Success(new Response.Builder().protocol(Protocol.HTTP_1_1).request(request).code(401).body(ResponseBody.create(MediaType.parse("UTF-8"), "Some error message")).build())

    val result = CustomerProfiler(canaryEmailAddress, nonOkResponseHttpClient, profileApiKey, profileHost)("whatever")
    result match {
      case Success(customerProfile) =>
        fail("Failure expected")
      case Failure(ex) =>
      //OK
    }
  }

  it should "Fail when response body is invalid" in {
    val badResponseHttpClient = (request: Request) =>
      Success(new Response.Builder().protocol(Protocol.HTTP_1_1).request(request).code(200).body(ResponseBody.create(MediaType.parse("UTF-8"), "{\"some\":\"value\"}")).build())

    val result = CustomerProfiler(canaryEmailAddress, badResponseHttpClient, profileApiKey, profileHost)("whatever")
    result match {
      case Success(customerProfile) =>
        fail("Failure expected")
      case Failure(ex) =>
      //OK
    }
  }

  it should "Succeed when response is valid" in {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")), StandardCharsets.UTF_8)
    val okResponseHttpClient = (request: Request) => {
      request.url.toString shouldBe s"$profileHost/customers/whatever"
      request.method shouldBe "GET"
      request.header("Authorization") shouldBe profileApiKey

      Success(new Response.Builder().protocol(Protocol.HTTP_1_1).request(request).code(200).body(ResponseBody.create(MediaType.parse("UTF-8"), validResponseJson)).build())
    }

    val result = CustomerProfiler(canaryEmailAddress, okResponseHttpClient, profileApiKey, profileHost)("whatever")
    result match {
      case Success(customerProfile) =>
        customerProfile shouldBe CustomerProfile(
          name = CustomerProfileName(
            title = Some("Mr"),
            firstName = "Gary",
            lastName = "Philpott",
            suffix = None
          ),
          emailAddresses = CustomerProfileEmailAddresses(
            primary = Some("qatesting@ovoenergy.com"),
            secondary = None
          ))
      case Failure(ex) =>
        fail(ex)
    }
  }


}
