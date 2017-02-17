package com.ovoenergy.orchestration.http

import okhttp3.{OkHttpClient, Request, Response}

import scala.util.Try

object HttpClient {

  private val httpClient = new OkHttpClient()

  def apply(request: Request): Try[Response] = {
    Try(httpClient.newCall(request).execute())
  }

}
