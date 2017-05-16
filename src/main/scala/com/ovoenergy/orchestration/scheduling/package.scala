package com.ovoenergy.orchestration

import java.time.{Clock, Instant, OffsetDateTime}

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.scheduling.TaskExecutor.log
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
    def buildFromTrigger(triggeredV3: TriggeredV3, clock: Clock = Clock.systemUTC()) = {
      Schedule(
        scheduleId = DynamoPersistence.generateScheduleId(),
        triggered = None,
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

    //TODO - Remove once migrated to V3 and no more schedules in DB
    def triggeredAsV3(schedule: Schedule): Option[TriggeredV3] = {

      def triggeredV2ToV3(triggeredV2: TriggeredV2): TriggeredV3 = {

        def metadataToV2(metadata: Metadata): MetadataV2 = {
          MetadataV2(
            createdAt = OffsetDateTime.parse(metadata.createdAt).toInstant,
            eventId = metadata.eventId,
            traceToken = metadata.traceToken,
            commManifest = metadata.commManifest,
            deliverTo = Customer(metadata.customerId),
            friendlyDescription = metadata.friendlyDescription,
            source = metadata.source,
            canary = metadata.canary,
            sourceMetadata = metadata.sourceMetadata.map(metadataToV2),
            triggerSource = metadata.triggerSource
          )
        }

        TriggeredV3(
          metadata = metadataToV2(triggeredV2.metadata),
          templateData = triggeredV2.templateData,
          deliverAt = triggeredV2.deliverAt.map(OffsetDateTime.parse(_).toInstant),
          expireAt = triggeredV2.expireAt.map(OffsetDateTime.parse(_).toInstant),
          preferredChannels = triggeredV2.preferredChannels
        )
      }

      (schedule.triggered, schedule.triggeredV3) match {
        case (Some(v2), _)    => Some(triggeredV2ToV3(v2))
        case (None, Some(v3)) => Some(v3)
        case (None, None)     => None
      }
    }
  }

  case class Schedule(
      scheduleId: ScheduleId,
      //TODO - Remove once migrated to V3 and no more schedules in DB
      triggered: Option[TriggeredV2],
      triggeredV3: Option[TriggeredV3],
      deliverAt: Instant,
      status: ScheduleStatus,
      history: Seq[Change],
      orchestrationExpiry: Instant,
      customerId: Option[String],
      commName: String
  )
}
