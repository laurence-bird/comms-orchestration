package com.ovoenergy.orchestration

import java.time.{Clock, Instant, OffsetDateTime}

import com.ovoenergy.orchestration.domain._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence
import com.ovoenergy.orchestration.kafka.consumers.EventConverter._

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
    def buildFromTrigger(triggeredV3: TriggeredV3, clock: Clock = Clock.systemUTC()) = {
      Schedule(
        scheduleId = DynamoPersistence.generateScheduleId(),
        triggeredV3 = Some(triggeredV3),
        deliverAt = triggeredV3.deliverAt.getOrElse(Instant.now(clock)),
        status = ScheduleStatus.Pending,
        customerId = triggeredV3.metadata.deliverTo match {
          case Customer(customerId) => Some(customerId)
          case _                    => None
        },
        commName = triggeredV3.metadata.commManifest.name,
        orchestrationExpiry = Instant.now(),
        history = Seq.empty[Change]
      )
    }
  }

  case class Schedule(
      scheduleId: ScheduleId,
      triggeredV3: Option[TriggeredV3],
      deliverAt: Instant,
      status: ScheduleStatus,
      history: Seq[Change],
      orchestrationExpiry: Instant,
      customerId: Option[String],
      commName: String
  ) extends LoggableEvent{
    override def loggableString: Option[String] = None
    override def mdcMap: Map[String, String] = Map(
      "scheduleId" -> scheduleId,
      "deliverAt" -> deliverAt.toString,
      "status" -> status.toString,
      "commName" -> commName,
    ) ++
      customerId.map(cId => Map("customerId" -> cId)).getOrElse(Map.empty) ++
      triggeredV3.map(_.mdcMap).getOrElse(Map.empty)
  }

  var counter = 1
  object ScheduleNew {
    def buildFromOld(old: Schedule) = {
      println(s"$counter: ${old.commName} - ${Hash(old.commName)}")
      counter += 1
      ScheduleNew(
        scheduleId = old.scheduleId,
        triggeredV4 = old.triggeredV3.map(_.toV4),
        deliverAt = old.deliverAt,
        status = old.status,
        customerId = old.customerId,
        templateId = Hash(old.commName),
        orchestrationExpiry = old.orchestrationExpiry,
        history = old.history
      )
    }
  }

  case class ScheduleNew(
                          scheduleId: ScheduleId,
                          triggeredV4: Option[TriggeredV4],
                          deliverAt: Instant,
                          status: ScheduleStatus,
                          history: Seq[Change],
                          orchestrationExpiry: Instant,
                          customerId: Option[String],
                          templateId: String
  ) extends LoggableEvent{
    override def loggableString: Option[String] = None
    override def mdcMap: Map[String, String] = Map(
      "scheduleId" -> scheduleId,
      "deliverAt" -> deliverAt.toString,
      "status" -> status.toString,
      "templateId" -> templateId,
    ) ++
      customerId.map(cId => Map("customerId" -> cId)).getOrElse(Map.empty) ++
      triggeredV4.map(_.mdcMap).getOrElse(Map.empty)
  }
}
