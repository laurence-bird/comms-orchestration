package com.ovoenergy.orchestration.scheduling

import java.time.Instant

object Restore {

  /**
    * Look in Dynamo for any pending schedules
    * and add scheduled tasks for each of them.
    *
    * @return a count of the number of tasks scheduled
    */
  def pickUpPendingSchedules(dynamo: Persistence.Listing, addSchedule: (ScheduleId, Instant) => Boolean): Int = {
    dynamo
      .listPendingSchedules()
      .map { (schedule: Schedule) =>
        addSchedule(schedule.scheduleId, schedule.deliverAt)
      }
      .count(identity)
  }

  /**
    * Look in Dynamo for any expired schedules
    * and add scheduled tasks for each of them.
    *
    * @return a count of the number of tasks scheduled
    */
  def pickUpExpiredSchedules(dynamo: Persistence.Listing, addSchedule: (ScheduleId, Instant) => Boolean): Int = {
    dynamo
      .listExpiredSchedules()
      .map { schedule =>
        addSchedule(schedule.scheduleId, schedule.deliverAt)
      }
      .count(identity)
  }

}
