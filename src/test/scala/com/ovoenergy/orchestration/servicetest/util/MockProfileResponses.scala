package com.ovoenergy.orchestration.serviceTest.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.mockserver.client.server.MockServerClient
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response

trait MockProfileResponses {

  def createOKCustomerProfileResponse(mockServerClient: MockServerClient) {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/GT-CUS-994332344")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

  def createInvalidCustomerProfileResponse(mockServerClient: MockServerClient) {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_missing_email_fields_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/GT-CUS-994332344")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

  def createBadCustomerProfileResponse(mockServerClient: MockServerClient) {
    mockServerClient.reset()
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/GT-CUS-994332344")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response("Some error")
          .withStatusCode(500)
      )
  }

  def createFlakyCustomerProfileResponse(mockServerClient: MockServerClient) {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")),
                 StandardCharsets.UTF_8)

    mockServerClient.reset()

    // Fail 3 times, then always succeed after that

    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/customers/GT-CUS-994332344")
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
          .withPath(s"/api/customers/GT-CUS-994332344")
          .withQueryStringParameter("apikey", "someApiKey")
      )
      .respond(
        response(validResponseJson)
          .withStatusCode(200)
      )
  }

}
