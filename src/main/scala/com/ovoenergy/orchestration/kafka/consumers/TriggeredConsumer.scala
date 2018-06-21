package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.TriggeredDataValidator
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.either._
import cats.syntax.flatMap._
import org.apache.kafka.clients.producer.RecordMetadata
import scala.concurrent.{ExecutionContext}

object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](scheduleTask: TriggeredV4 => F[Either[ErrorDetails, Boolean]],
                         sendFailedEvent: FailedV3 => F[Unit],
                         sendOrchestrationStartedEvent: OrchestrationStartedV3 => F[RecordMetadata],
                         generateTraceToken: () => String,
                         orchestrateComm: (TriggeredV4, InternalMetadata) => F[Either[ErrorDetails, RecordMetadata]])(
      implicit ec: ExecutionContext): TriggeredV4 => F[Unit] = { triggeredV4: TriggeredV4 =>
    def scheduleOrFail(triggeredV4: TriggeredV4) = {
      scheduleTask(triggeredV4) flatMap {
        case Right(r)  => Async[F].pure(())
        case Left(err) => sendFailedEvent(failedEventFromErr(err))
      }
    }

    def handleOrchestrationResult(either: Either[ErrorDetails, RecordMetadata]): F[Unit] = either match {
      case Left(err) =>
        warn(triggeredV4)(s"Error orchestrating comm: ${err.reason}")
        sendFailedEvent(failedEventFromErr(err))
      case Right(_) => Async[F].pure(())
    }

    def orchestrateOrFail(triggeredV4: TriggeredV4): F[Unit] = {
      val internalMetadata = buildInternalMetadata()
      for {
        _          <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggeredV4.metadata, internalMetadata))
        orchResult <- orchestrateComm(triggeredV4, internalMetadata)
        _          <- handleOrchestrationResult(orchResult)
      } yield ()
    }

    def buildInternalMetadata() = InternalMetadata(generateTraceToken())

    def failedEventFromErr(err: ErrorDetails): FailedV3 = {
      FailedV3(
        MetadataV3.fromSourceMetadata("orchestration", triggeredV4.metadata),
        buildInternalMetadata(),
        err.reason,
        err.errorCode
      )
    }

    def isScheduled(triggeredV4: TriggeredV4) = triggeredV4.deliverAt.isDefined

    val validatedTriggeredV4 = TriggeredDataValidator(triggeredV4)

    validatedTriggeredV4 match {
      case Left(err) => sendFailedEvent(failedEventFromErr(err))
      case Right(t) if isScheduled(t) =>
        scheduleOrFail(t)
      case Right(t) =>
        orchestrateOrFail(t)
    }
  }
}
