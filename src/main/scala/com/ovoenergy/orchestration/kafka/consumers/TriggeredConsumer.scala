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
import com.ovoenergy.orchestration.domain.FailureDetails
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
    def scheduleOrFail(triggered: TriggeredV4) = {
      scheduleTask(triggered) flatMap {
        case Right(r) => {
          // TODO Clean this up
          issueFeedback.send(
            Feedback(
              triggered.metadata.commId,
              extractCustomer(triggered.metadata.deliverTo),
              FeedbackOptions.Scheduled,
              Some(s"Comm Scheduled for delivery"),
              None,
              None,
              EventMetadata.fromMetadata(triggered.metadata, Hash(triggered.metadata.eventId))
            ))
        }
        case Left(err) => {
          issueFeedback.send(failureDetailsFromErr(err))
        }
      }
    }

    def handleOrchestrationResult(either: Either[ErrorDetails, RecordMetadata]): F[Unit] = either match {
      case Left(err) =>
        warn(triggered)(s"Error orchestrating comm: ${err.reason}")
        issueFeedback.send(failureDetailsFromErr(err)).void
      case Right(_) => Async[F].pure(())
    }

    def orchestrateOrFail(triggeredV4: TriggeredV4): F[Unit] = {
      val internalMetadata = buildInternalMetadata()
      for {
        // TODO Clean this up
        _ <- issueFeedback.send(
          Feedback(
            triggered.metadata.commId,
            extractCustomer(triggered.metadata.deliverTo),
            FeedbackOptions.Scheduled,
            Some(s"Comm Scheduled for delivery"),
            None,
            None,
            EventMetadata.fromMetadata(triggered.metadata, Hash(triggered.metadata.eventId))
          ))
        _          <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggeredV4.metadata, internalMetadata))
        orchResult <- orchestrateComm(triggeredV4, internalMetadata)
        _          <- handleOrchestrationResult(orchResult)
      } yield ()
    }

    def buildInternalMetadata() = InternalMetadata(generateTraceToken())

    def failureDetailsFromErr(err: ErrorDetails): FailureDetails = {
      FailureDetails(
        MetadataV3.fromSourceMetadata("orchestration", triggered.metadata, Hash(triggered.metadata.eventId)),
        err.reason,
        err.errorCode,
        buildInternalMetadata()
      )
    }

    def isScheduled(triggered: TriggeredV4) = triggered.deliverAt.isDefined

    val validatedTriggeredV4 = TriggeredDataValidator(triggered)

    validatedTriggeredV4 match {
      case Left(err) => issueFeedback.sendWithLegacy(failureDetailsFromErr(err)).void
      case Right(t) if isScheduled(t) =>
        scheduleOrFail(t).void
      case Right(t) =>
        orchestrateOrFail(t)
    }
  }
}
