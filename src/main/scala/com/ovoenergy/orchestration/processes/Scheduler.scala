package com.ovoenergy.orchestration.processes

import java.time.{Clock, Instant}

import com.ovoenergy.comms.model.{ErrorCode, TriggeredV2}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId}

import scala.util.control.NonFatal

object Scheduler {

  def apply(storeSchedule: (Schedule) => _, scheduleTask: (ScheduleId, Instant) => _, clock: Clock = Clock.systemUTC())
           (triggered: TriggeredV2): Either[ErrorDetails, _] = {

    try {
      val schedule = Schedule.buildFromTrigger(triggered, clock)
      val scheduleInstant = schedule.deliverAt
      storeSchedule(schedule)
      scheduleTask(schedule.scheduleId, scheduleInstant)
      Right(())
    } catch {
      case NonFatal(e) => Left(ErrorDetails(s"Error scheduling comm: ${e.getMessage}", ErrorCode.OrchestrationError))
    }
  }
}
