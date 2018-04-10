package servicetest.helpers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.ovoenergy.orchestration.util.TestUtil
import org.mockserver.client.server.MockServerClient
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response

import scala.concurrent.duration._

trait MockProfileResponses {

  def waitUntilMockServerIsRunning(mockServerClient: MockServerClient): Unit = {
    val deadline = 5.seconds.fromNow

    def loop(): Unit = {
      if (deadline.isOverdue())
        sys.error("Mock server didn't start up in time")

      if (!mockServerClient.isRunning) {
        Thread.sleep(200)
        loop()
      }
    }

    loop()
  }

  def createOKCustomerProfileResponse(mockServerClient: MockServerClient) {
    waitUntilMockServerIsRunning(mockServerClient)

    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/${TestUtil.customerId}")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

  def createInvalidCustomerProfileResponse(mockServerClient: MockServerClient) {
    waitUntilMockServerIsRunning(mockServerClient)

    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_missing_email_fields_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/${TestUtil.customerId}")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

  def createBadCustomerProfileResponse(mockServerClient: MockServerClient) {
    waitUntilMockServerIsRunning(mockServerClient)

    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/${TestUtil.customerId}")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response("Some error")
          .withStatusCode(500)
      )
  }

  def createFlakyCustomerProfileResponse(mockServerClient: MockServerClient) {
    waitUntilMockServerIsRunning(mockServerClient)

    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()

    // Fail 3 times, then always succeed after that

    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/${TestUtil.customerId}")
          .withQueryStringParameter("apikey", "someApiKey"),
        Times.exactly(3)
      )
      .respond(
        response("Some error")
          .withStatusCode(500)
      )

    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/${TestUtil.customerId}")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

}
