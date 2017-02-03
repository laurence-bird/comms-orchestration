package com.ovoenergy.orchestration.scheduling

import java.time.format.DateTimeParseException
import java.time.{Clock, Instant, ZoneId, ZonedDateTime}
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo._
import com.gu.scanamo.error.ConditionNotMet
import com.gu.scanamo.query.{AndCondition, Condition, UniqueKey}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model.{CommType, TemplateData}
import com.ovoenergy.orchestration.scheduling.ScheduleStatus
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.generic.semiauto._
import org.slf4j.LoggerFactory

object Persistence {

  sealed trait SetAsOrchestratingResult
  case object Successful extends SetAsOrchestratingResult
  case object AlreadyBeingOrchestrated extends SetAsOrchestratingResult
  case object Failed extends SetAsOrchestratingResult

  private val log = LoggerFactory.getLogger("Persistence")

  implicit val uuidDynamoFormat = DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val zonedDateTimeDynamoFormat = DynamoFormat.coercedXmap[ZonedDateTime, Long, DateTimeParseException] (
    (millis) => {ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))}
  )(
    _.toInstant.toEpochMilli
  )

  implicit val scheduleStatusDynamoFormat = DynamoFormat.coercedXmap[ScheduleStatus, String, MatchError]{
    case "Pending" => ScheduleStatus.Pending
    case "Orchestrating" => ScheduleStatus.Orchestrating
    case "Complete" => ScheduleStatus.Complete
    case "Failed" => ScheduleStatus.Failed
    case "Cancelled" => ScheduleStatus.Cancelled
  }{
    case ScheduleStatus.Pending => "Pending"
    case ScheduleStatus.Orchestrating => "Orchestrating"
    case ScheduleStatus.Complete => "Complete"
    case ScheduleStatus.Failed => "Failed"
    case ScheduleStatus.Cancelled => "Cancelled"
  }

  implicit val commTypeDynamoFormat = DynamoFormat.coercedXmap[CommType, String, MatchError]{
    case "Service" => CommType.Service
    case "Regulatory" => CommType.Regulatory
    case "Marketing" => CommType.Marketing
  }{
    case CommType.Service => "Service"
    case CommType.Regulatory => "Regulatory"
    case CommType.Marketing => "Marketing"
  }

  import io.circe.shapes._
  implicit val templateDataDecoder: Decoder[TemplateData] = deriveDecoder[TemplateData]
  implicit val templateDataEncoder: Encoder[TemplateData] = deriveEncoder[TemplateData]

  implicit val templateDataFormat = DynamoFormat.coercedXmap[TemplateData, String, IllegalArgumentException](
    (string) => {
      parse(string) match {
        case Right(json) =>
          val result = templateDataDecoder.decodeJson(json)
          result match {
            case Right(td) => td
            case Left(error) =>
              log.warn("Unable to deserialize to TemplateData", error)
              throw new IllegalArgumentException("Unable to deserialize to TemplateData", error)
          }
        case Left(error) =>
          log.warn("Unable to deserialize to TemplateData", error)
          throw new IllegalArgumentException("Unable to deserialize to TemplateData", error)
      }
    }
  )(
    (templateData) => templateDataEncoder.apply(templateData).spaces2
  )

}

class Persistence(context: Context, clock: Clock = Clock.systemUTC()) {
  import Persistence._

  private val log = LoggerFactory.getLogger("Persistence")

  def storeSchedule(commSchedule: Schedule): Unit = {
    Scanamo.exec(context.db)(context.table.put(commSchedule))
  }

  def retrieveSchedule(scheduleId: String): Option[Schedule] = {
    Scanamo.get[Schedule](context.db)(context.table.name)('scheduleId -> scheduleId) match {
      case Some(Left(error))               =>
        log.warn(s"Problem retrieving schedule: $scheduleId", error)
        None
      case Some(Right(schedule: Schedule)) => Some(schedule)
      case None                            => None
    }
  }

  def attemptSetScheduleAsOrchestrating(scheduleId: String): SetAsOrchestratingResult = {
    val now = ZonedDateTime.now(clock)
    val operation = context.table
      .given(
        Condition('status -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toInstant.toEpochMilli)
      )
      .update('scheduleId -> scheduleId,
        set('status -> (ScheduleStatus.Orchestrating: ScheduleStatus))
          and set('orchestrationExpiry, now.plusMinutes(5).toInstant.toEpochMilli)
          and append('history, Change(now, "Start orchestrating"))
      )
    Scanamo.exec(context.db)(operation) match {
      case Left(ConditionNotMet(e)) => AlreadyBeingOrchestrated
      case Left(error)              =>
        log.warn(s"Problem marking schedule as orchestrating: $scheduleId", error)
        Failed
      case Right(_)                 => Successful
    }
  }

  def setScheduleAsFailed(scheduleId: String, reason: String) = {
    val now = ZonedDateTime.now(clock)
    Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Failed: ScheduleStatus))
        and append('history, Change(now, s"Failed - $reason"))
    ))
  }

  def setScheduleAsComplete(scheduleId: String) = {
    val now = ZonedDateTime.now(clock)
    Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Complete: ScheduleStatus))
        and append('history, Change(now, "Orchestration complete"))
    ))
  }

  def retrievePendingSchedules(): List[Schedule] = {
    val now = ZonedDateTime.now(clock).toInstant.toEpochMilli
    val pending = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Pending: ScheduleStatus)))
    val expired = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Orchestrating: ScheduleStatus) and 'orchestrationExpiry < now))

    pending ++ expired flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        None
    }
  }

  def cancelSchedules(customerId: String, commName: String): Unit = {
    val schedules = Scanamo.exec(context.db)(context.table.index("customerId-commName-index").query('customerId -> customerId and ('commName beginsWith commName)))
    schedules.foreach {
      case Right(schedule) => if (schedule.commName == commName) cancelSchedule(schedule.scheduleId.toString)
      case Left(e)         => log.warn("Problem retrieving pending schedule", e)
    }
  }

  private def cancelSchedule(scheduleId: String)  = {
    val now = ZonedDateTime.now(clock)
    val operation = context.table
      .given(
        Condition('status -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toInstant.toEpochMilli)
      )
      .update('scheduleId -> scheduleId,
        set('status -> (ScheduleStatus.Cancelled: ScheduleStatus))
          and append('history, Change(now, "Cancelled"))
      )

    val result = Scanamo.exec(context.db)(operation)
    if (result.isLeft && !result.left.get.isInstanceOf[ConditionNotMet])
      log.warn(s"Problem cancelling schedule: $scheduleId", result.left.get)
  }
}

case class Context(db: AmazonDynamoDB, table: Table[Schedule])

object Context{
  import Persistence._
  def apply(db: AmazonDynamoDB, tableName: String): Context = {
    Context(
      db,
      Table[Schedule](tableName)
    )
  }
}