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

  def apply[F[_]: Async](scheduleTask: (TriggeredV3) => F[Either[ErrorDetails, Boolean]],
                         sendFailedEvent: FailedV2 => F[Unit],
                         sendOrchestrationStartedEvent: OrchestrationStartedV2 => F[RecordMetadata],
                         generateTraceToken: () => String,
                         orchestrateComm: (TriggeredV3, InternalMetadata) => F[Either[ErrorDetails, RecordMetadata]])(
      implicit ec: ExecutionContext): TriggeredV3 => F[Unit] = { triggeredV3: TriggeredV3 =>
    def scheduleOrFail(triggeredV3: TriggeredV3) = {
      scheduleTask(triggeredV3) flatMap {
        case Right(r)  => Async[F].pure(())
        case Left(err) => sendFailedEvent(failedEventFromErr(err))
      }
    }

    def handleOrchestrationResult(either: Either[ErrorDetails, RecordMetadata]): F[Unit] = either match {
      case Left(err) =>
        warn(triggeredV3)(s"Error orchestrating comm: ${err.reason}")
        sendFailedEvent(failedEventFromErr(err))
      case Right(_) => Async[F].pure(())
    }

    def orchestrateOrFail(triggeredV3: TriggeredV3): F[Unit] = {
      val internalMetadata = buildInternalMetadata()
      for {
        _          <- sendOrchestrationStartedEvent(OrchestrationStartedV2(triggeredV3.metadata, internalMetadata))
        orchResult <- orchestrateComm(triggeredV3, internalMetadata)
        _          <- handleOrchestrationResult(orchResult)
      } yield ()
    }

    def buildInternalMetadata() = InternalMetadata(generateTraceToken())

    def failedEventFromErr(err: ErrorDetails): FailedV2 = {
      FailedV2(
        MetadataV2.fromSourceMetadata("orchestration", triggeredV3.metadata),
        buildInternalMetadata(),
        err.reason,
        err.errorCode
      )
    }

    def isScheduled(triggeredV3: TriggeredV3) = triggeredV3.deliverAt.isDefined

    val validatedTriggeredV3 = TriggeredDataValidator(triggeredV3)

    validatedTriggeredV3 match {
      case Left(err) => sendFailedEvent(failedEventFromErr(err))
      case Right(t) if isScheduled(t) =>
        scheduleOrFail(t)
      case Right(t) =>
        orchestrateOrFail(t)
    }
  }
}
