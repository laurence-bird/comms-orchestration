package com.ovoenergy.orchestration.profile

import cats.effect.{Async, ConcurrentEffect, IO, Resource}
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.ovoenergy.orchestration.domain._
import io.circe.generic.auto._
import cats.implicits._
import com.ovoenergy.comms.model.OrchestrationError
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.profile.CustomerProfiler.ProfileCustomer
import com.ovoenergy.orchestration.util.Retry
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s._
import org.http4s.Method._
import org.http4s.Status.ServerError
import org.http4s.circe._
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait CustomerProfiler[F[_]] {
  def apply(profileCustomer: ProfileCustomer): F[Either[ErrorDetails, CustomerProfile]]
}

object CustomerProfiler extends LoggingWithMDC {

  case class CustomerProfileResponse(name: CustomerProfileName,
                                     emailAddress: Option[String],
                                     phoneNumber: Option[String],
                                     communicationPreferences: Seq[CommunicationPreference])

  case class ProfileCustomer(customerId: String, canary: Boolean, traceToken: String, commId: String)
  case class ProfileCustomerRequest(pc: ProfileCustomer, uri: Uri)

  implicit val loggableProfilerCustomerRequest = Loggable.instance[ProfileCustomerRequest] { profileCustomerRequest =>
    Map(
      "isCanary"   -> profileCustomerRequest.pc.canary.toString,
      "traceToken" -> profileCustomerRequest.pc.traceToken,
      "customerId" -> profileCustomerRequest.pc.customerId,
      "uri"        -> profileCustomerRequest.uri.renderString,
      "commId"     -> profileCustomerRequest.pc.commId
    )
  }

  case class ServerErrorException(message: String) extends Exception(message)

  def apply[F[_]: Async](client: Client[F], uri: Uri, retry: Retry[F], apiKey: String)(implicit ec: ExecutionContext) =
    new CustomerProfiler[F] {
      val dsl = new Http4sClientDsl[F] {}
      import dsl._

      override def apply(profileCustomer: ProfileCustomer) = {
        def toCustomerProfile(response: CustomerProfileResponse) = {
          CustomerProfile(
            response.name,
            response.communicationPreferences,
            ContactProfile(response.emailAddress.map(EmailAddress), response.phoneNumber.map(MobilePhoneNumber), None)
          ) // TODO: Get customer address
        }

        implicit val decoder = jsonOf[F, CustomerProfileResponse]

        val baseuri = uri
          ./(s"api")
          ./(s"customers")
          ./(profileCustomer.customerId)
          .withQueryParam("apikey", apiKey)

        val fullUri: Uri = {
          if (profileCustomer.canary)
            baseuri
              .withQueryParam("canary", "true")
          else
            baseuri
        }

        val req = ProfileCustomerRequest(profileCustomer, fullUri)

        val response: F[Either[ErrorDetails, CustomerProfile]] =
          Async[F].delay(info(req)(s"Requesting customer profile")) >> client.fetch(GET(fullUri)) {
            response: Response[F] =>
              {
                Async[F].delay(info("Received response")) >>
                  (if (response.status.isSuccess) {
                     response.as[CustomerProfileResponse].map(c => Right(toCustomerProfile(c)))
                   } else {
                     response.as[String].flatMap { str =>
                       response.status.responseClass match {
                         case ServerError =>
                           Async[F].delay(
                             warn(req)(s"Error response (${response.status.code}), from profile service $str")
                           ) >>
                             Async[F].raiseError(ServerErrorException(
                               s"Error response (${response.status.code}), from profile service $str"))
                         case _ =>
                           Async[F].delay(
                             warn(req)(s"Error response (${response.status.code}), from profile service $str")) >>
                             Async[F].delay(
                               Left(ErrorDetails(
                                 s"Error response (${response.status.code}) retrieving customer profile: $str",
                                 OrchestrationError)))
                       }
                     }
                   })
              }
          }
        retry(response, _.isInstanceOf[ServerErrorException]).onError {
          case NonFatal(e) =>
            Async[F].delay(failWithException(req)("Failed to retrieve customer profile")(e))
        }
      }
    }

  def resource[F[_]: ConcurrentEffect](retry: Retry[F], apiKey: String, profileUri: Uri)(
      implicit executionContext: ExecutionContext): Resource[F, CustomerProfiler[F]] = {
    BlazeClientBuilder[F](executionContext)
      .withResponseHeaderTimeout(60.seconds)
      .resource
      .map { httpClient =>
        CustomerProfiler[F](
          client = httpClient,
          uri = profileUri,
          retry = retry,
          apiKey = apiKey
        )
      }
  }
}
