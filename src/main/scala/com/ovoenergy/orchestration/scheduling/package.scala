package com.ovoenergy.orchestration

import java.time.{Clock, Instant}

import com.ovoenergy.comms.model.TriggeredV2
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence

package object scheduling {

  type ScheduleId = String

  sealed trait ScheduleStatus
  object ScheduleStatus {
    case object Pending       extends ScheduleStatus
    case object Orchestrating extends ScheduleStatus
    case object Complete      extends ScheduleStatus
    case object Failed        extends ScheduleStatus
    case object Cancelled     extends ScheduleStatus
  }

  case class Change(timestamp: Instant, operation: String)

  object Schedule {
    def buildFromTrigger(triggeredV2: TriggeredV2, clock: Clock = Clock.systemUTC()) = {
      Schedule(
        scheduleId = DynamoPersistence.generateScheduleId(),
        triggered = triggeredV2,
        deliverAt = triggeredV2.deliverAt.map(Instant.parse(_)).getOrElse(Instant.now(clock)),
        status = ScheduleStatus.Pending,
        customerId = triggeredV2.metadata.customerId,
        commName = triggeredV2.metadata.commManifest.name,
        orchestrationExpiry = Instant.now(),
        history = Seq.empty[Change]
      )
    }
  }

  case class Schedule(
      scheduleId: ScheduleId,
      triggered: TriggeredV2,
      deliverAt: Instant,
      status: ScheduleStatus,
      history: Seq[Change],
      orchestrationExpiry: Instant,
      customerId: String,
      commName: String
  )
}
