package com.ovoenergy.orchestration.scheduling

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  Successful,
  Failed => FailedPersistence
}
import org.apache.kafka.clients.producer.RecordMetadata
import cats.implicits._
import com.ovoenergy.comms.templates.util.Hash

import scala.concurrent.duration._
import scala.util.control.NonFatal

object TaskExecutor extends LoggingWithMDC {
  def execute(persistence: Persistence.Orchestration,
              orchestrateTrigger: (TriggeredV4, InternalMetadata) => IO[Either[ErrorDetails, RecordMetadata]],
              sendOrchestrationStartedEvent: OrchestrationStartedV3 => IO[RecordMetadata],
              generateTraceToken: () => String,
              sendFailedEvent: FailedV3 => IO[RecordMetadata])(scheduleId: ScheduleId): Unit = {

    def buildAndSendFailedEvent(reason: String,
                                triggered: TriggeredV4,
                                errorCode: ErrorCode,
                                internalMetadata: InternalMetadata): IO[RecordMetadata] = {

      // TODO: Send Feedback event here
      sendFailedEvent(
        FailedV3(MetadataV3.fromSourceMetadata("orchestration", triggered.metadata, Hash(triggered.metadata.eventId)),
                 internalMetadata,
                 reason,
                 errorCode))
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
              .flatMap(_ => buildAndSendFailedEvent(err.reason, triggered, err.errorCode, internalMetadata))
              .map(_ => ())
            // TODO: Raise Feedback event here (Failed)
          }
        }

        val result = r.unsafeRunTimed(20.seconds)

        if (result.isDefined) ()
        else {
          warn(triggered)("Orchestrating comm timed out, the comm may still get orchestrated, raising failed event")
          persistence.setScheduleAsFailed(scheduleId, "Orchestrating comm timed out")
          buildAndSendFailedEvent("Orchestrating comm timed out", triggered, OrchestrationError, internalMetadata)
        }
      } catch {
        case NonFatal(e) =>
          warnWithException(triggered)("Unable to orchestrate comm, raising failed event")(e)
          buildAndSendFailedEvent(e.getMessage, triggered, OrchestrationError, internalMetadata)
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
