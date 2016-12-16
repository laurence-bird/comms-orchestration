package com.ovoenergy.orchestration.http

import io.circe.Decoder
import io.circe.parser._

import scala.util.{Failure, Success, Try}

object JsonDecoding {

  def decodeJson[T: Decoder](jsonBody: String): Try[T] = {
    decode[T](jsonBody) match {
      case Right(data) => Success(data)
      case Left(error) => Failure(new Exception(s"Invalid JSON: $error"))
    }
  }

}
