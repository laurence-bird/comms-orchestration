package com.ovoenergy.orchestration

import java.time.{Clock, Instant, OffsetDateTime}

import com.ovoenergy.orchestration.domain._
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence

package object scheduling extends LoggingWithMDC {

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
    def buildFromTrigger(triggeredV4: TriggeredV4, clock: Clock = Clock.systemUTC()) = {
      Schedule(
        scheduleId = DynamoPersistence.generateScheduleId(),
        triggeredV4 = Some(triggeredV4),
        deliverAt = triggeredV4.deliverAt.getOrElse(Instant.now(clock)),
        status = ScheduleStatus.Pending,
        customerId = triggeredV4.metadata.deliverTo match {
          case Customer(customerId) => Some(customerId)
          case _                    => None
        },
        templateId = triggeredV4.metadata.templateManifest.id,
        orchestrationExpiry = Instant.now(),
        history = Seq.empty[Change]
      )
    }
  }

  case class Schedule(scheduleId: ScheduleId,
                      triggeredV4: Option[TriggeredV4],
                      deliverAt: Instant,
                      status: ScheduleStatus,
                      history: Seq[Change],
                      orchestrationExpiry: Instant,
                      customerId: Option[String],
                      templateId: String)
      extends LoggableEvent {
    override def loggableString: Option[String] = None
    override def mdcMap: Map[String, String] =
      Map(
        "scheduleId" -> scheduleId,
        "deliverAt"  -> deliverAt.toString,
        "status"     -> status.toString,
        "templateId" -> templateId,
      ) ++
        customerId.map(cId => Map("customerId" -> cId)).getOrElse(Map.empty) ++
        triggeredV4.map(_.mdcMap).getOrElse(Map.empty)
  }
}
