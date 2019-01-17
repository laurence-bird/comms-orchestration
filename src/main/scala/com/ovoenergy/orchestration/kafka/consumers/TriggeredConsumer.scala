package com.ovoenergy.orchestration.kafka.consumers

import java.time.ZonedDateTime
import cats.effect.Sync
import cats.implicits._
import com.ovoenergy.comms.model._
import com.ovoenergy.kafka.common.event.EventMetadata
import com.ovoenergy.orchestration.domain.BuildFeedback.{extractCustomer, _}
import com.ovoenergy.orchestration.domain.{CommId, EventId, FailureDetails, InternalFailure, TraceToken}
import com.ovoenergy.orchestration.kafka.producers.IssueFeedback
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.{Orchestrator, TriggeredDataValidator}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext

//TODO: Find a better name for this, responsibility is unclear from the name
object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]](scheduleTask: TriggeredV4 => F[Either[ErrorDetails, Boolean]],
                  issueFeedback: IssueFeedback[F],
                  sendOrchestrationStartedEvent: OrchestrationStartedV3 => F[RecordMetadata],
                  generateTraceToken: () => String,
                  orchestrator: Orchestrator[F])(implicit ec: ExecutionContext, F: Sync[F]): TriggeredV4 => F[Unit] = {
    triggered: TriggeredV4 =>
      def scheduleOrFail(triggered: TriggeredV4, internalMetadata: InternalMetadata) = {
        scheduleTask(triggered) flatMap {
          case Right(r) => {
            issueFeedback.send(
              Feedback(
                triggered.metadata.commId,
                Some(triggered.metadata.friendlyDescription),
                extractCustomer(triggered.metadata.deliverTo),
                FeedbackOptions.Scheduled,
                Some(s"Comm scheduled for delivery"),
                None,
                None,
                Some(triggered.metadata.templateManifest),
                EventMetadata.fromMetadata(triggered.metadata, triggered.metadata.commId ++ "-feedback-scheduled")
              ))
          }
          case Left(err) => {
            issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggered.metadata, internalMetadata)
          }
        }
      }

      def sendFeedbackIfFailure(either: Either[ErrorDetails, RecordMetadata],
                                internalMetadata: InternalMetadata,
                                triggeredV4: TriggeredV4): F[Unit] = either match {
        case Left(err) =>
          F.delay(warn(triggered)(s"Error orchestrating comm: ${err.reason}")) *>
            issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggeredV4.metadata, internalMetadata).void
        case Right(_) => ().pure[F]
      }

      def orchestrateOrFail(triggeredV4: TriggeredV4, internalMetadata: InternalMetadata): F[Unit] = {

        for {
          _ <- issueFeedback.send(
            Feedback(
              triggered.metadata.commId,
              Some(triggered.metadata.friendlyDescription),
              extractCustomer(triggered.metadata.deliverTo),
              FeedbackOptions.Pending,
              Some(s"Trigger for communication accepted"),
              None,
              None,
              Some(triggered.metadata.templateManifest),
              EventMetadata.fromMetadata(triggered.metadata, triggered.metadata.commId ++ "-feedback-pending")
            ))
          _          <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggeredV4.metadata, internalMetadata))
          orchResult <- orchestrator(triggeredV4, internalMetadata)
          _          <- sendFeedbackIfFailure(orchResult, internalMetadata, triggeredV4)
        } yield ()
      }

      def buildInternalMetadata() = InternalMetadata(generateTraceToken())

      def failureDetailsFromErr(err: ErrorDetails): FailureDetails = {
        FailureDetails(
          triggered.metadata.deliverTo,
          CommId(triggered.metadata.commId),
          triggered.metadata.friendlyDescription,
          triggered.metadata.templateManifest,
          TraceToken(triggered.metadata.traceToken),
          EventId(triggered.metadata.eventId),
          err.reason,
          err.errorCode,
          InternalFailure
        )
      }

      def isScheduled(triggered: TriggeredV4) = triggered.deliverAt.isDefined

      def isOutOfDate(triggeredV4: TriggeredV4): Boolean =
        triggered.metadata.createdAt
          .isBefore(
            ZonedDateTime.now().minusDays(7).toInstant
          )

      val internalMetadata     = buildInternalMetadata()
      val validatedTriggeredV4 = TriggeredDataValidator(triggered)
      validatedTriggeredV4 match {
        case Left(err) =>
          issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggered.metadata, internalMetadata).void
        case Right(t) if isOutOfDate(t) =>
          sendFeedbackIfFailure(Left(ErrorDetails("Trigger message is more than a week old", CommExpired)),
                                internalMetadata,
                                t)
        case Right(t) if isScheduled(t) =>
          scheduleOrFail(t, internalMetadata).void
        case Right(t) =>
          orchestrateOrFail(t, internalMetadata)
      }
  }
}
