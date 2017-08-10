package com.ovoenergy.orchestration.scheduling

import java.util.concurrent.TimeoutException

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  Failed => FailedPersistence,
  Successful
}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.kafka.clients.producer.RecordMetadata
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

object TaskExecutor extends LoggingWithMDC {
  def execute(persistence: Persistence.Orchestration,
              orchestrateTrigger: (TriggeredV3, InternalMetadata) => Either[ErrorDetails, Future[RecordMetadata]],
              sendOrchestrationStartedEvent: OrchestrationStartedV2 => Future[RecordMetadata],
              generateTraceToken: () => String,
              sendFailedEvent: FailedV2 => Future[RecordMetadata])(scheduleId: ScheduleId): Unit = {

    def failed(reason: String,
               triggered: TriggeredV3,
               errorCode: ErrorCode,
               internalMetadata: InternalMetadata): Unit = {
      persistence.setScheduleAsFailed(scheduleId, reason)
      val future = sendFailedEvent(FailedV2(triggered.metadata, internalMetadata, reason, errorCode))
      try {
        Await.result(future, 5.seconds)
      } catch {
        case NonFatal(e) => logWarn(triggered.metadata.traceToken, "Unable to send a failed event", e)
      }
    }

    def awaitOrchestrationFuture(triggered: TriggeredV3,
                                 internalMetadata: InternalMetadata,
                                 f: Future[RecordMetadata]): Unit = {
      try {
        Await.result(f, 10.seconds)
        persistence.setScheduleAsComplete(scheduleId)
      } catch {
        case e: TimeoutException =>
          logWarn(triggered.metadata.traceToken,
                  "Orchestrating comm timed out, the comm may still get orchestrated, raising failed event",
                  e)
          failed("Orchestrating comm timed out", triggered, OrchestrationError, internalMetadata)
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
        log.warn(
          s"Unable to orchestrate scheduleId: $scheduleId, failed to mark as orchestrating in persistence. Unable to raise a failed event.")
      case Successful(schedule: Schedule) =>
        Schedule.triggeredAsV3(schedule) match {
          case Some(triggered) =>
            sendOrchestrationStartedEvent(OrchestrationStartedV2(triggered.metadata, internalMetadata))
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
