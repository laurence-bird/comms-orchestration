package com.ovoenergy.orchestration
package kafka
package consumers

import java.time.{ZonedDateTime, ZoneOffset, Instant}
import scala.concurrent.ExecutionContext

import cats.effect.Sync
import cats.implicits._

import org.apache.kafka.clients.producer.RecordMetadata

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.deduplication.ProcessingStore
import com.ovoenergy.kafka.common.event.EventMetadata
import domain.BuildFeedback.{extractCustomer, _}
import domain.{CommId, EventId, FailureDetails, InternalFailure, TraceToken}
import kafka.producers.IssueFeedback
import logging.LoggingWithMDC
import processes.Orchestrator.ErrorDetails
import processes.{Orchestrator, TriggeredDataValidator}

//TODO: Find a better name for this, responsibility is unclear from the name
object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]](scheduleTask: TriggeredV4 => F[Either[ErrorDetails, Boolean]],
                  issueFeedback: IssueFeedback[F],
                  generateTraceToken: () => String,
                  orchestrator: Orchestrator[F],
                  deduplication: ProcessingStore[F, String],
                  hash: Hash[F])(implicit ec: ExecutionContext, F: Sync[F]): TriggeredV4 => F[Unit] = {
    triggered: TriggeredV4 =>
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

      def handleEvent(now: Instant, internalMetadata: InternalMetadata) = {

        def orchestrateOrFail(triggeredV4: TriggeredV4): F[Unit] = {

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
            orchResult <- orchestrator(triggeredV4, internalMetadata)
            _          <- sendFeedbackIfFailure(orchResult, triggeredV4)
          } yield ()
        }

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

        def sendFeedbackIfFailure(either: Either[ErrorDetails, RecordMetadata], triggeredV4: TriggeredV4): F[Unit] =
          either match {
            case Left(err) =>
              F.delay(warn(triggered)(s"Error orchestrating comm: ${err.reason}")) *>
                issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggeredV4.metadata, internalMetadata).void
            case Right(_) => ().pure[F]
          }

        def isScheduled(triggered: TriggeredV4) = triggered.deliverAt.isDefined

        def isOutOfDate(triggeredV4: TriggeredV4): Boolean =
          triggered.metadata.createdAt
            .isBefore(
              now.atZone(ZoneOffset.UTC).minusDays(7).toInstant
            )

        TriggeredDataValidator(triggered) match {
          case Left(err) =>
            issueFeedback.sendWithLegacy(failureDetailsFromErr(err), triggered.metadata, internalMetadata).void
          case Right(t) if isOutOfDate(t) =>
            sendFeedbackIfFailure(Left(ErrorDetails("Trigger message is expired", CommExpired)), t)
          case Right(t) if isScheduled(t) =>
            scheduleOrFail(t, internalMetadata).void
          case Right(t) =>
            orchestrateOrFail(t)
        }
      }

      def sendDuplicateFailure(internalMetadata: InternalMetadata) = {
        issueFeedback
          .sendWithLegacy(
            failureDetailsFromErr(
              ErrorDetails(
                s"The communication with id: ${triggered.metadata.commId} is a duplicate",
                DuplicateCommError
              )),
            triggered.metadata,
            internalMetadata
          )
          .void
      }

      for {
        now              <- F.delay(Instant.now())
        internalMetadata <- F.delay(InternalMetadata(generateTraceToken()))
        hashedEvent      <- hash(triggered)
        result <- deduplication.protect(
          hashedEvent,
          handleEvent(now, internalMetadata),
          sendDuplicateFailure(internalMetadata)
        )
      } yield result
  }
}
