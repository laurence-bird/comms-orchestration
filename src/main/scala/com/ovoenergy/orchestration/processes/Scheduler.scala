package com.ovoenergy.orchestration.processes

import java.time.{Clock, Instant}

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId}
import cats.syntax.either._
import scala.util.Try

object Scheduler extends LoggingWithMDC {
  type CustomerId = String
  type CommName   = String

  def scheduleComm(storeInDb: (Schedule) => Unit,
                   registerTask: (ScheduleId, Instant) => Boolean,
                   clock: Clock = Clock.systemUTC())(triggered: TriggeredV3): Either[ErrorDetails, Boolean] = {
    val result = Try {
      log.info(s"Scheduling triggered event: $triggered")
      val schedule        = Schedule.buildFromTrigger(triggered, clock)
      val scheduleInstant = schedule.deliverAt
      storeInDb(schedule)
      registerTask(schedule.scheduleId, scheduleInstant)
    }

    Either
      .fromTry(result)
      .leftMap(e => ErrorDetails("Failed to schedule comm", OrchestrationError))
  }

  def descheduleComm(removeFromDb: (CustomerId, CommName) => Seq[Either[ErrorDetails, Schedule]],
                     removeTask: (ScheduleId) => Boolean)(
      cancellationRequested: CancellationRequestedV2): Seq[Either[ErrorDetails, MetadataV2]] = {

    def removeScheduleFromMemory(schedule: Schedule): Either[ErrorDetails, MetadataV2] = {
      // Filter out failed schedule removals
      if (removeTask(schedule.scheduleId)) {
        Schedule.triggeredAsV3(schedule) match {
          case Some(triggered) => Right(triggered.metadata)
          case None =>
            Left(
              ErrorDetails(s"Unable to orchestrate as no Triggered event in Schedule: $schedule", OrchestrationError))
        }
      } else {
        Left(ErrorDetails(s"Failed to remove ${schedule.scheduleId} schedule(s) from memory", OrchestrationError))
      }
    }
    log.debug(s"Processing request: $cancellationRequested")
    val dynamoResult = removeFromDb(cancellationRequested.customerId, cancellationRequested.commName)
    dynamoResult.map { schedule =>
      log.debug(s"Removing schedule from memory: $schedule")
      schedule.flatMap(removeScheduleFromMemory)
    }
  }
}
