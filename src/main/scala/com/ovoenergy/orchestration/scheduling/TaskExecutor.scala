package com.ovoenergy.orchestration.scheduling

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  Successful,
  Failed => FailedPersistence
}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.util.control.NonFatal

object TaskExecutor extends LoggingWithMDC {
  def execute(persistence: Persistence.Orchestration,
              orchestrateTrigger: (TriggeredV3, InternalMetadata) => Either[ErrorDetails, IO[RecordMetadata]],
              sendOrchestrationStartedEvent: OrchestrationStartedV2 => IO[RecordMetadata],
              generateTraceToken: () => String,
              sendFailedEvent: FailedV2 => IO[RecordMetadata],
              ec: ExecutionContext)(scheduleId: ScheduleId): Unit = {

    implicit val executionContext = ec

    def failed(reason: String,
               triggered: TriggeredV3,
               errorCode: ErrorCode,
               internalMetadata: InternalMetadata): Unit = {
      persistence.setScheduleAsFailed(scheduleId, reason)
      val future = sendFailedEvent(
        FailedV2(MetadataV2.fromSourceMetadata("orchestration", triggered.metadata),
                 internalMetadata,
                 reason,
                 errorCode))
      try {
        future.unsafeRunTimed(5.seconds).getOrElse {
          logWarn(triggered.metadata.traceToken, "Unable to send a failed event")

        }
      } catch {
        case NonFatal(e) => logWarn(triggered.metadata.traceToken, "Unable to send a failed event", e)
      }
    }

    def awaitOrchestrationFuture(triggered: TriggeredV3,
                                 internalMetadata: InternalMetadata,
                                 f: IO[RecordMetadata]): Unit = {
      try {
        val result = f.unsafeRunTimed(10.seconds)
        if (result.isDefined)
          persistence.setScheduleAsComplete(scheduleId)
        else {
          logWarn(triggered.metadata.traceToken,
                  "Orchestrating comm timed out, the comm may still get orchestrated, raising failed event")
          failed("Orchestrating comm timed out", triggered, OrchestrationError, internalMetadata)
        }
      } catch {
        case NonFatal(e) =>
          logWarn(triggered.metadata.traceToken, "Unable to orchestrate comm, raising failed event", e)
          failed(e.getMessage, triggered, OrchestrationError, internalMetadata)
      }
    }

    log.debug(s"Orchestrating commm (scheduleId: $scheduleId)")
    val internalMetadata = InternalMetadata(generateTraceToken())
    persistence.attemptSetScheduleAsOrchestrating(scheduleId) match {
      case AlreadyBeingOrchestrated => ()
      case FailedPersistence =>
        log.error(
          s"Unable to orchestrate scheduleId: $scheduleId, failed to mark as orchestrating in persistence. Unable to raise a failed event.")
      case Successful(schedule: Schedule) =>
        schedule.triggeredV3 match {
          case Some(triggered) =>
            sendOrchestrationStartedEvent(OrchestrationStartedV2(triggered.metadata, internalMetadata))
              .unsafeRunTimed(5.seconds)
            orchestrateTrigger(triggered, internalMetadata) match {
              case Right(f) => awaitOrchestrationFuture(triggered, internalMetadata, f)
              case Left(e)  => failed(e.reason, triggered, e.errorCode, internalMetadata)
            }
          case None =>
            val failureReason = s"Unable to orchestrate as no Triggered event in Schedule: $schedule"
            log.warn(failureReason)
            persistence.setScheduleAsFailed(scheduleId, failureReason)
        }
    }
  }
}
