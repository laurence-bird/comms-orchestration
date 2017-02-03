package com.ovoenergy.orchestration.scheduling

import java.time.{Clock, DateTimeException, Instant}
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, TypeCoercionError}
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model.TemplateData
import com.ovoenergy.orchestration.scheduling.Persistence.Context
import io.circe.{Decoder, Encoder, Error}
import io.circe.parser._
import io.circe.generic.semiauto._
import org.slf4j.LoggerFactory

object Persistence {

  sealed trait SetAsOrchestratingResult
  case object Successful extends SetAsOrchestratingResult
  case object AlreadyBeingOrchestrated extends SetAsOrchestratingResult
  case object Failed extends SetAsOrchestratingResult

  implicit val uuidDynamoFormat = DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val instantDynamoFormat = DynamoFormat.coercedXmap[Instant, Long, DateTimeException](Instant.ofEpochMilli)(_.toEpochMilli)

  import io.circe.shapes._
  implicit val templateDataDecoder: Decoder[TemplateData] = deriveDecoder[TemplateData]
  implicit val templateDataEncoder: Encoder[TemplateData] = deriveEncoder[TemplateData]

  import cats.syntax.either._
  implicit val templateDataFormat = DynamoFormat.xmap[TemplateData, String](
    (string) => {
      val decodedTemplateData: Either[Error, TemplateData] = for {
        json <- parse(string)
        templateData <- templateDataDecoder.decodeJson(json)
      } yield templateData
      decodedTemplateData.leftMap(error => {TypeCoercionError(error)})
    }
  )(
    (templateData) => templateDataEncoder.apply(templateData).spaces2
  )

  case class Context(db: AmazonDynamoDB, table: Table[Schedule])

  object Context{
    def apply(db: AmazonDynamoDB, tableName: String): Context = {
      Context(
        db,
        Table[Schedule](tableName)
      )
    }
  }
}

class Persistence(orchestrationExpiryMinutes: Int, context: Context, clock: Clock = Clock.systemUTC()) {
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
    val now = Instant.now(clock)
    val operation = context.table
      .given(
        Condition('status -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toEpochMilli)
      )
      .update('scheduleId -> scheduleId,
        set('status -> (ScheduleStatus.Orchestrating: ScheduleStatus))
          and set('orchestrationExpiry, now.plusSeconds(60 * orchestrationExpiryMinutes).toEpochMilli)
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
    val now = Instant.now(clock)
    Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Failed: ScheduleStatus))
        and append('history, Change(now, s"Failed - $reason"))
    ))
  }

  def setScheduleAsComplete(scheduleId: String) = {
    val now = Instant.now(clock)
    Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Complete: ScheduleStatus))
        and append('history, Change(now, "Orchestration complete"))
    ))
  }

  def retrievePendingSchedules(): List[Schedule] = {
    val now = Instant.now(clock).toEpochMilli
    val pending = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Pending: ScheduleStatus)))
    val expired = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Orchestrating: ScheduleStatus) and 'orchestrationExpiry < now))

    pending ++ expired flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        None
    }
  }

  def cancelSchedules(customerId: String, commName: String): List[Schedule] = {
    val schedules = Scanamo.exec(context.db)(context.table.index("customerId-commName-index").query('customerId -> customerId and ('commName beginsWith commName)))
    schedules.flatMap {
      case Right(schedule) =>
        if (schedule.commName == commName) attemptToCancelSchedule(schedule.scheduleId.toString)
        else None
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        None
    }
  }

  private def attemptToCancelSchedule(scheduleId: String): Option[Schedule] = {
    val now = Instant.now(clock)
    val operation = context.table
      .given(
        Condition('status -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toEpochMilli)
      )
      .update('scheduleId -> scheduleId,
        set('status -> (ScheduleStatus.Cancelled: ScheduleStatus))
          and append('history, Change(now, "Cancelled"))
      )

    Scanamo.exec(context.db)(operation) match {
      case Right(schedule)          => Some(schedule)
      case Left(ConditionNotMet(_)) => None
      case Left(error)              =>
        log.warn(s"Problem cancelling schedule: $scheduleId", error)
        None
    }
  }
}