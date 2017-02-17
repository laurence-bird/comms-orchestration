package com.ovoenergy.orchestration.scheduling

import java.util.concurrent.TimeoutException

import com.ovoenergy.comms.model.{ErrorCode, InternalMetadata, Metadata, TriggeredV2}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Failed, Successful}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object TaskExecutor extends LoggingWithMDC {
  def execute(persistence: Persistence.Orchestration, orchestrateTrigger: (TriggeredV2, InternalMetadata) => Either[ErrorDetails, Future[_]],
              generateTraceToken: () => String, sendFailedEvent: (String, TriggeredV2, ErrorCode, InternalMetadata) => Future[_])
             (scheduleId: ScheduleId)(implicit ec: ExecutionContext): Unit = {

    def failed(reason: String, triggered: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata): Unit = {
      persistence.setScheduleAsFailed(scheduleId, reason)
      val future = sendFailedEvent(reason, triggered, errorCode, internalMetadata)
      try {
        Await.result(future, 5.seconds)
      } catch {
        case NonFatal(e)  => logWarn(triggered.metadata.traceToken, "Unable to send a failed event", e)
      }
    }

    def awaitOrchestrationFuture(triggered: TriggeredV2, internalMetadata: InternalMetadata, f: Future[_]): Unit = {
      try {
        Await.result(f, 10.seconds)
        persistence.setScheduleAsComplete(scheduleId)
      } catch {
          case e: TimeoutException =>
            logWarn(triggered.metadata.traceToken, "Orchestrating comm timed out, the comm may still get orchestrated, raising failed event", e)
            failed("Orchestrating comm timed out", triggered, ErrorCode.OrchestrationError, internalMetadata)
          case NonFatal(e) =>
            logWarn(triggered.metadata.traceToken, "Unable to orchestrate comm, raising failed event", e)
            failed(e.getMessage, triggered, ErrorCode.OrchestrationError, internalMetadata)
        }
    }

    log.debug(s"Orchestrating commm (scheduleId: $scheduleId)")
    val internalMetadata = InternalMetadata(generateTraceToken())
    persistence.attemptSetScheduleAsOrchestrating(scheduleId) match {
      case AlreadyBeingOrchestrated => ()
      case Failed                   => log.warn(s"Unable to orchestrate scheduleId: $scheduleId, failed to mark as orchestrating in persistence. Unable to raise a failed event.")
      case Successful(schedule)     => orchestrateTrigger(schedule.triggered, internalMetadata) match {
        case Right(f) => awaitOrchestrationFuture(schedule.triggered, internalMetadata, f)
        case Left(e)  => failed(e.reason, schedule.triggered, e.errorCode, internalMetadata)
      }
    }
  }

  override def loggerName: String = "Scheduled Task Execution"
}
