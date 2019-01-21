package com.ovoenergy.orchestration.profile

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import cats.effect.IO
import cats.effect._
import cats.implicits._
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.ovoenergy.comms.model._
import org.scalatest._
import WireMock.{get, _}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.http.UniformDistribution
import com.ovoenergy.orchestration.domain.{CustomerProfile => CProfile}
import com.ovoenergy.orchestration.domain.{
  CommunicationPreference,
  ContactProfile,
  CustomerProfileName,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.profile.CustomerProfiler.{ProfileCustomer, ServerErrorException}
import com.ovoenergy.orchestration.util.Retry
import org.http4s.{InvalidMessageBodyFailure, Uri}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

class CustomerProfilerSpec
    extends FlatSpec
    with Matchers
    with EitherValues
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with TryValues
    with Assertions {

  val profileApiKey = "apiKey"
  val profileHost   = "http://somehost.com"
  val traceToken    = "token"
  val commId        = "id"

  lazy val uri   = Uri.unsafeFromString(s"http://localhost:${wireMockServer.port()}/yolo")
  val customerId = "whatever"
  val path       = s"/yolo/api/customers/$customerId"
  val validResponseJson =
    new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")), StandardCharsets.UTF_8)

  import cats.effect.IO._

  implicit lazy val ec: ExecutionContext                    = scala.concurrent.ExecutionContext.Implicits.global
  implicit lazy val t: Timer[IO]                            = timer(ec)
  implicit lazy val concurrenctEffect: ConcurrentEffect[IO] = cats.effect.IO.ioConcurrentEffect(contextShift(ec))

  lazy val customerProfiler = CustomerProfiler.resource[IO](Retry.fixed[IO](4, 10.millisecond), profileApiKey, uri)

  behavior of "Customer Profiler"

  val wireMockServer: WireMockServer = new WireMockServer(wireMockConfig().dynamicPort())

  it should "Return an appropriate error code for 4xx (non-recoverable errors), without retry" in {
    stubFor(
      get(urlPathEqualTo(path))
        .withQueryParam("apikey", equalTo(profileApiKey))
        .willReturn(
          aResponse()
            .withRandomDelay(new UniformDistribution(300, 5000))
            .withBody("You're not allowed here")
            .withStatus(401)))

    customerProfiler
      .use(_.apply(ProfileCustomer("whatever", canary = false, traceToken, commId)))
      .unsafeRunSync()
      .left
      .value shouldBe ErrorDetails("Error response (401) retrieving customer profile: You're not allowed here",
                                   OrchestrationError)

    verify(1,
           getRequestedFor(urlPathEqualTo(path))
             .withQueryParam("apikey", equalTo(profileApiKey)))
  }

  it should "Fail when response is not a success code, with retries" in {
    stubFor(
      get(urlPathEqualTo(path))
        .withQueryParam("apikey", equalTo(profileApiKey))
        .willReturn(
          aResponse()
            .withRandomDelay(new UniformDistribution(300, 5000))
            .withBody(s"Profile service is dead")
            .withStatus(500)))

    val resultIo = customerProfiler
      .use(_.apply(ProfileCustomer("whatever", canary = false, traceToken, commId)))

    val result = Try(resultIo.unsafeRunSync())
    verify(5,
           getRequestedFor(urlPathEqualTo(path))
             .withQueryParam("apikey", equalTo(profileApiKey)))

    result.failure.exception.isInstanceOf[ServerErrorException]
  }

  it should "Fail when response body is invalid" in {
    stubFor(
      get(urlPathEqualTo(path))
        .withQueryParam("apikey", equalTo(profileApiKey))
        .willReturn(
          aResponse()
            .withRandomDelay(new UniformDistribution(300, 5000))
            .withBody(s"""{\"some\":\"value\"}""")
            .withStatus(200)))

    val resultIo = customerProfiler
      .use(_.apply(ProfileCustomer("whatever", canary = false, traceToken, commId)))

    assertThrows[InvalidMessageBodyFailure](resultIo.unsafeRunSync())
  }

  it should "Succeed when response is valid" in {
    stubFor(
      get(urlPathEqualTo(path))
        .withQueryParam("apikey", equalTo(profileApiKey))
        .willReturn(
          aResponse()
            .withRandomDelay(new UniformDistribution(300, 5000))
            .withBody(validResponseJson)
            .withStatus(200)))

    val resultIo = customerProfiler
      .use(_.apply(ProfileCustomer("whatever", canary = false, traceToken, commId)))

    resultIo.unsafeRunSync() shouldBe Right(
      CProfile(
        name = CustomerProfileName(
          title = Some("Mr"),
          firstName = "John",
          lastName = "Wayne",
          suffix = None
        ),
        communicationPreferences = Seq(
          CommunicationPreference(
            Service,
            Seq(SMS, Email)
          )
        ),
        ContactProfile(
          emailAddress = Some(EmailAddress("qatesting@ovoenergy.com")),
          mobileNumber = Some(MobilePhoneNumber("+447985631544")),
          postalAddress = None
        )
      )
    )
  }

  it should "Ask the profiles service for the canary when requested" in {
    stubFor(
      get(urlPathEqualTo(path))
        .withQueryParam("canary", equalTo("true"))
        .withQueryParam("apikey", equalTo(profileApiKey))
        .willReturn(
          aResponse()
            .withRandomDelay(new UniformDistribution(300, 5000))
            .withBody(validResponseJson)
            .withStatus(200)))

    val result: Either[ErrorDetails, CProfile] = customerProfiler
      .use(_.apply(ProfileCustomer("whatever", canary = true, traceToken, commId)))
      .unsafeRunSync()

    result match {
      case Right(customerProfile) => succeed
      // ok
      case Left(err) =>
        fail(s"Unexpected failure: ${err.reason}")
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    wireMockServer.resetAll()
    WireMock.configureFor(wireMockServer.port())
  }

  override protected def afterEach(): Unit = {
    wireMockServer.resetAll()
    super.afterEach()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    wireMockServer.start()
  }

  override protected def afterAll(): Unit = {
    wireMockServer.shutdown()
    super.afterAll()
  }
}
