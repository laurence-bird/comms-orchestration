package com.ovoenergy.orchestration

import java.time.Instant
import java.util.UUID

import com.ovoenergy.comms.model.TriggeredV2

package object scheduling {

  sealed trait ScheduleStatus
  object ScheduleStatus {
    case object Pending extends ScheduleStatus
    case object Orchestrating extends ScheduleStatus
    case object Complete extends ScheduleStatus
    case object Failed extends ScheduleStatus
    case object Cancelled extends ScheduleStatus
  }

  case class Change(timestamp: Instant, operation: String)

  case class Schedule(
                           scheduleId: UUID,
                           triggered: TriggeredV2,
                           deliverAt: Instant,
                           status: ScheduleStatus,
                           history: Seq[Change],
                           orchestrationExpiry: Instant,
                           customerId: String,
                           commName: String
                     )
}
