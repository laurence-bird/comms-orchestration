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
import com.ovoenergy.comms.templates.util.Hash
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](scheduleTask: TriggeredV4 => F[Either[ErrorDetails, Boolean]],
                         sendFailedEvent: FailedV3 => F[Unit],
                         sendOrchestrationStartedEvent: OrchestrationStartedV3 => F[RecordMetadata],
                         generateTraceToken: () => String,
                         orchestrateComm: (TriggeredV4, InternalMetadata) => F[Either[ErrorDetails, RecordMetadata]])(
      implicit ec: ExecutionContext): TriggeredV4 => F[Unit] = { triggered: TriggeredV4 =>
    def scheduleOrFail(triggered: TriggeredV4) = {
      scheduleTask(triggered) flatMap {
        case Right(r) => {
          // TODO: Send feedback event here (Scheduled)
          Async[F].pure(())
        }
        case Left(err) => {
          // TODO: Send Feedback event here (Failed)
          sendFailedEvent(failedEventFromErr(err))
        }
      }
    }

    def handleOrchestrationResult(either: Either[ErrorDetails, RecordMetadata]): F[Unit] = either match {
      case Left(err) =>
        warn(triggered)(s"Error orchestrating comm: ${err.reason}")
        // TODO: Send Feedback event here (Failed)
        sendFailedEvent(failedEventFromErr(err))
      case Right(_) => Async[F].pure(())
    }

    def orchestrateOrFail(triggeredV4: TriggeredV4): F[Unit] = {
      val internalMetadata = buildInternalMetadata()
      for {
        //TODO: Send Feedback event here (Pending)
        _          <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggeredV4.metadata, internalMetadata))
        orchResult <- orchestrateComm(triggeredV4, internalMetadata)
        _          <- handleOrchestrationResult(orchResult)
      } yield ()
    }

    def buildInternalMetadata() = InternalMetadata(generateTraceToken())

    def failedEventFromErr(err: ErrorDetails): FailedV3 = {
      FailedV3(
        MetadataV3.fromSourceMetadata("orchestration", triggered.metadata, Hash(triggered.metadata.eventId)),
        buildInternalMetadata(),
        err.reason,
        err.errorCode
      )
    }

    def isScheduled(triggered: TriggeredV4) = triggered.deliverAt.isDefined

    val validatedTriggeredV4 = TriggeredDataValidator(triggered)

    validatedTriggeredV4 match {
      case Left(err) => sendFailedEvent(failedEventFromErr(err))
      case Right(t) if isScheduled(t) =>
        scheduleOrFail(t)
      case Right(t) =>
        orchestrateOrFail(t)
    }
  }
}
