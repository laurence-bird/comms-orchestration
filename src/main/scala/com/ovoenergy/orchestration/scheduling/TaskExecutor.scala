package com.ovoenergy.orchestration.scheduling

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.domain.BuildFeedback._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  Successful,
  Failed => FailedPersistence
}
import org.apache.kafka.clients.producer.RecordMetadata
import cats.implicits._
import com.ovoenergy.orchestration.domain.{FailureDetails, InternalFailure}
import com.ovoenergy.orchestration.kafka.IssueFeedback

import scala.concurrent.duration._
import scala.util.control.NonFatal

object TaskExecutor extends LoggingWithMDC {
  def execute(persistence: Persistence.Orchestration,
              orchestrateTrigger: (TriggeredV4, InternalMetadata) => IO[Either[ErrorDetails, RecordMetadata]],
              sendOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata],
              generateTraceToken: () => String,
              issueFeedback: IssueFeedback[IO])(scheduleId: ScheduleId): Unit = {

    def buildAndSendFailedEvents(triggered: TriggeredV4,
                                 errorDetails: ErrorDetails,
                                 internalMetadata: InternalMetadata): IO[RecordMetadata] = {
      issueFeedback.sendWithLegacy(
        FailureDetails(
          triggered.metadata.deliverTo,
          triggered.metadata.commId,
          triggered.metadata.traceToken,
          triggered.metadata.eventId,
          errorDetails.reason,
          errorDetails.errorCode,
          InternalFailure
        ),
        triggered.metadata,
        internalMetadata
      )
    }

    def awaitOrchestrationFuture(triggered: TriggeredV4,
                                 internalMetadata: InternalMetadata,
                                 f: IO[Either[ErrorDetails, _]]): Unit = {
      try {
        val r = f.flatMap {
          case Right(r) =>
            IO(persistence.setScheduleAsComplete(scheduleId))
          case Left(err) => {
            warn(triggered)(s"Failed to orchestrate comm: ${err.reason}")
            IO(persistence.setScheduleAsFailed(scheduleId, err.reason))
              .flatMap(_ => buildAndSendFailedEvents(triggered, err, internalMetadata))
              .map(_ => ())
          }
        }

        val result = r.unsafeRunTimed(20.seconds)

        if (result.isDefined) ()
        else {
          warn(triggered)("Orchestrating comm timed out, the comm may still get orchestrated, raising failed event")
          persistence.setScheduleAsFailed(scheduleId, "Orchestrating comm timed out")
          buildAndSendFailedEvents(triggered,
                                   ErrorDetails("Orchestrating comm timed out", OrchestrationError),
                                   internalMetadata)
        }
      } catch {
        case NonFatal(e) =>
          warnWithException(triggered)("Unable to orchestrate comm, raising failed event")(e)
          buildAndSendFailedEvents(triggered, ErrorDetails(e.getMessage, OrchestrationError), internalMetadata)
      }
    }

    log.debug(s"Orchestrating comm (scheduleId: $scheduleId)")
    val internalMetadata = InternalMetadata(generateTraceToken())
    persistence.attemptSetScheduleAsOrchestrating(scheduleId) match {
      case AlreadyBeingOrchestrated => ()
      case FailedPersistence =>
        log.error(
          s"Unable to orchestrate scheduleId: $scheduleId, failed to mark as orchestrating in persistence. Unable to raise a failed event.")
      case Successful(schedule: Schedule) =>
        schedule.triggeredV4 match {
          case Some(triggered) => {
            val orchResult: IO[Either[ErrorDetails, RecordMetadata]] = for {
              _   <- sendOrchestrationStartedEvent(OrchestrationStartedV3(triggered.metadata, internalMetadata))
              res <- orchestrateTrigger(triggered, internalMetadata)
            } yield res

            awaitOrchestrationFuture(triggered, internalMetadata, orchResult)
          }
          case None =>
            val failureReason = s"Unable to orchestrate as no Triggered event in Schedule: $schedule"
            log.warn(failureReason)
            persistence.setScheduleAsFailed(scheduleId, failureReason)
        }
    }
  }
}
