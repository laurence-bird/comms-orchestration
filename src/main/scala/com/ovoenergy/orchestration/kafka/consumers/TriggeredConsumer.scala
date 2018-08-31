package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.kafka.common.event.EventMetadata
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.TriggeredDataValidator
import com.ovoenergy.orchestration.domain.BuildFeedback.{extractCustomer, _}
import com.ovoenergy.orchestration.domain.{FailureDetails, InternalFailure}
import com.ovoenergy.orchestration.kafka.IssueFeedback
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](scheduleTask: TriggeredV4 => F[Either[ErrorDetails, Boolean]],
                         issueFeedback: IssueFeedback[F],
                         sendOrchestrationStartedEvent: OrchestrationStartedV3 => F[RecordMetadata],
                         generateTraceToken: () => String,
                         orchestrateComm: (TriggeredV4, InternalMetadata) => F[Either[ErrorDetails, RecordMetadata]])(
      implicit ec: ExecutionContext): TriggeredV4 => F[Unit] = { triggered: TriggeredV4 =>
    def scheduleOrFail(triggered: TriggeredV4, internalMetadata: InternalMetadata) = {
      scheduleTask(triggered) flatMap {
        case Right(r) => {
          issueFeedback.send(
            Feedback(
              triggered.metadata.commId,
              extractCustomer(triggered.metadata.deliverTo),
              FeedbackOptions.Scheduled,
              Some(s"Comm scheduled for delivery"),
              None,
              None,
              EventMetadata.fromMetadata(triggered.metadata, Hash(triggered.metadata.eventId))
            ))
        }
        case Left(err) => {
          issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggered.metadata, internalMetadata)
        }
      }
    }

    def orchestrateOrFail(triggeredV4: TriggeredV4, internalMetadata: InternalMetadata): F[Unit] = {

      def sendFeedbackIfFailure(either: Either[ErrorDetails, RecordMetadata],
                                internalMetadata: InternalMetadata): F[Unit] = either match {
        case Left(err) =>
          warn(triggered)(s"Error orchestrating comm: ${err.reason}")
          issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggeredV4.metadata, internalMetadata).void
        case Right(_) => Async[F].pure(())
      }

      for {
        _ <- issueFeedback.send(
          Feedback(
            triggered.metadata.commId,
            extractCustomer(triggered.metadata.deliverTo),
            FeedbackOptions.Pending,
            Some(s"Trigger for communication accepted"),
            None,
            None,
            EventMetadata.fromMetadata(triggered.metadata, Hash(triggered.metadata.eventId))
          ))
        _          <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggeredV4.metadata, internalMetadata))
        orchResult <- orchestrateComm(triggeredV4, internalMetadata)
        _          <- sendFeedbackIfFailure(orchResult, internalMetadata)
      } yield ()
    }

    def buildInternalMetadata() = InternalMetadata(generateTraceToken())

    def failureDetailsFromErr(err: ErrorDetails): FailureDetails = {
      FailureDetails(
        triggered.metadata.deliverTo,
        triggered.metadata.commId,
        triggered.metadata.traceToken,
        triggered.metadata.eventId,
        err.reason,
        err.errorCode,
        InternalFailure
      )
    }

    def isScheduled(triggered: TriggeredV4) = triggered.deliverAt.isDefined
    val internalMetadata                    = buildInternalMetadata()
    val validatedTriggeredV4                = TriggeredDataValidator(triggered)
    validatedTriggeredV4 match {
      case Left(err) =>
        issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggered.metadata, internalMetadata).void
      case Right(t) if isScheduled(t) =>
        scheduleOrFail(t, internalMetadata).void
      case Right(t) =>
        orchestrateOrFail(t, internalMetadata)
    }
  }
}
