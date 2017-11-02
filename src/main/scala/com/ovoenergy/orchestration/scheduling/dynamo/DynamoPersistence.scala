package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Clock, DateTimeException, Instant}
import java.util.{UUID, Map => JMap}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, DynamoReadError, TypeCoercionError}
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  Failed,
  SetAsOrchestratingResult,
  Successful
}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.Context
import com.ovoenergy.orchestration.scheduling.{Change, ScheduleStatus, _}
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.{Decoder, Encoder, Error}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object DynamoPersistence {

  def generateScheduleId(): ScheduleId = {
    UUID.randomUUID().toString
  }

  implicit val uuidDynamoFormat =
    DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val instantDynamoFormat =
    DynamoFormat.coercedXmap[Instant, Long, DateTimeException](Instant.ofEpochMilli)(_.toEpochMilli)

  implicit val scheduleStatusDynamoFormat = DynamoFormat.coercedXmap[ScheduleStatus, String, MatchError] {
    case "Pending"       => ScheduleStatus.Pending
    case "Orchestrating" => ScheduleStatus.Orchestrating
    case "Complete"      => ScheduleStatus.Complete
    case "Failed"        => ScheduleStatus.Failed
    case "Cancelled"     => ScheduleStatus.Cancelled
  } {
    case ScheduleStatus.Pending       => "Pending"
    case ScheduleStatus.Orchestrating => "Orchestrating"
    case ScheduleStatus.Complete      => "Complete"
    case ScheduleStatus.Failed        => "Failed"
    case ScheduleStatus.Cancelled     => "Cancelled"
  }

  implicit val commTypeDynamoFormat = DynamoFormat.coercedXmap[CommType, String, MatchError] {
    case "Service"    => Service
    case "Regulatory" => Regulatory
    case "Marketing"  => Marketing
  } {
    case Service    => "Service"
    case Regulatory => "Regulatory"
    case Marketing  => "Marketing"
  }

  import io.circe.shapes._
  implicit val templateDataDecoder: Decoder[TemplateData] = deriveDecoder[TemplateData]
  implicit val templateDataEncoder: Encoder[TemplateData] = deriveEncoder[TemplateData]

  import cats.syntax.either._
  implicit val templateDataFormat = DynamoFormat.xmap[TemplateData, String](
    (string) => {
      val decodedTemplateData: Either[Error, TemplateData] = for {
        json         <- parse(string)
        templateData <- templateDataDecoder.decodeJson(json)
      } yield templateData
      decodedTemplateData.leftMap(error => { TypeCoercionError(error) })
    }
  )(
    (templateData) => templateDataEncoder.apply(templateData).spaces2
  )

  case class Context(db: AmazonDynamoDB, table: Table[Schedule])

  object Context {
    def apply(db: AmazonDynamoDB, tableName: String): Context = {
      Context(
        db,
        Table[Schedule](tableName)
      )
    }
  }
}

class DynamoPersistence(orchestrationExpiryMinutes: Int, context: Context, clock: Clock = Clock.systemUTC())
    extends Persistence.Orchestration
    with Persistence.Listing {

  import DynamoPersistence._

  private val log = LoggerFactory.getLogger("Persistence")

  def storeSchedule(commSchedule: Schedule): Unit = {
    log.info(s"persisting schedule: ${commSchedule}")
    Scanamo.exec(context.db)(context.table.put(commSchedule))
    log.info(s"Persisted comm schedule: $commSchedule")
  }

  def retrieveSchedule(scheduleId: ScheduleId): Option[Schedule] = {
    Scanamo.get[Schedule](context.db)(context.table.name)('scheduleId -> scheduleId) match {
      case Some(Left(error)) =>
        log.warn(s"Problem retrieving schedule: $scheduleId", error)
        None
      case Some(Right(schedule: Schedule)) => Some(schedule)
      case None                            => None
    }
  }

  def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
    val now = Instant.now(clock)
    val operation = context.table
      .given(
        Condition('status         -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toEpochMilli)
      )
      .update(
        'scheduleId -> scheduleId,
        set('status -> (ScheduleStatus.Orchestrating: ScheduleStatus))
          and set('orchestrationExpiry, now.plusSeconds(60 * orchestrationExpiryMinutes).toEpochMilli)
          and append('history, Change(now, "Start orchestrating"))
      )
    Scanamo.exec(context.db)(operation) match {
      case Left(ConditionNotMet(e)) => AlreadyBeingOrchestrated
      case Left(error) =>
        log.warn(s"Problem marking schedule as orchestrating: $scheduleId", error)
        Failed
      case Right(schedule) => Successful(schedule)
    }
  }

  def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
    val now = Instant.now(clock)
    val result = Scanamo.exec(context.db)(
      context.table.update('scheduleId -> scheduleId,
                           set('status -> (ScheduleStatus.Failed: ScheduleStatus))
                             and append('history, Change(now, s"Failed - $reason"))))
    result match {
      case Left(error) => log.warn(s"Error marking schedule as Failed $error")
      case Right(_)    => ()
    }
  }

  def setScheduleAsComplete(scheduleId: ScheduleId): Unit = {
    val now = Instant.now(clock)
    val result = Scanamo.exec(context.db)(
      context.table.update('scheduleId -> scheduleId,
                           set('status -> (ScheduleStatus.Complete: ScheduleStatus))
                             and append('history, Change(now, "Orchestration complete"))))
    result match {
      case Left(error) => log.warn(s"Error marking schedule as Complete $error")
      case Right(_)    => ()
    }
  }

  def listPendingSchedules(): List[Schedule] = {
    val pending = Scanamo.exec(context.db)(
      context.table
        .index("status-orchestrationExpiry-index")
        .query('status -> (ScheduleStatus.Pending: ScheduleStatus)))

    pending flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        None
    }
  }

  def listExpiredSchedules(): List[Schedule] = {
    val now = Instant.now(clock).toEpochMilli
    val expired = Scanamo.exec(context.db)(
      context.table
        .index("status-orchestrationExpiry-index")
        .query('status -> (ScheduleStatus.Orchestrating: ScheduleStatus) and 'orchestrationExpiry < now))

    expired flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving expired schedule", e)
        None
    }
  }

  def cancelSchedules(customerId: String, commName: String): Seq[Either[ErrorDetails, Schedule]] = {
    log.info(s"Removing schedule for $customerId, $commName")
    val now       = Instant.now(clock)
    val db        = context.db
    val tableName = context.table.name
    val query = new QueryRequest()
      .withTableName(tableName)
      .withIndexName("customerId-commName-index")
      .addExpressionAttributeNamesEntry("#customerId", "customerId")
      .addExpressionAttributeValuesEntry(":customerId", new AttributeValue(customerId))
      .addExpressionAttributeNamesEntry("#commName", "commName")
      .addExpressionAttributeValuesEntry(":commName", new AttributeValue(commName))
      .withKeyConditionExpression("#customerId = :customerId and #commName = :commName")
      .addExpressionAttributeNamesEntry("#status", "status")
      .addExpressionAttributeNamesEntry("#expiry", "orchestrationExpiry")
      .addExpressionAttributeValuesEntry(":pending", scheduleStatusDynamoFormat.write(ScheduleStatus.Pending))
      .addExpressionAttributeValuesEntry(":orchestrating",
                                         scheduleStatusDynamoFormat.write(ScheduleStatus.Orchestrating))
      .addExpressionAttributeValuesEntry(":now", instantDynamoFormat.write(now))
      .withFilterExpression("#status = :pending or (#status = :orchestrating and #expiry < :now)")

    @tailrec
    def pageQuery(key: JMap[String, AttributeValue],
                  currentItems: Vector[JMap[String, AttributeValue]]): Vector[JMap[String, AttributeValue]] = {
      query.setExclusiveStartKey(key)
      val queryResult   = db.query(query)
      val evaluationKey = queryResult.getLastEvaluatedKey
      val items         = currentItems ++ queryResult.getItems.asScala.toVector
      if (evaluationKey == null) items
      else pageQuery(evaluationKey, items)
    }

    val items = pageQuery(null, Vector.empty[JMap[String, AttributeValue]])
      .map(item => {
        DynamoFormat[Schedule].read(new AttributeValue().withM(item))
      })

    log.info(s"Found ${items.length} schedules to cancel")

    items.flatMap {
      case Right(schedule) =>
        attemptToCancelSchedule(schedule.scheduleId)
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        Some(Left(ErrorDetails("Failed to deserialise pending schedule", OrchestrationError)))
    }
  }

  private def attemptToCancelSchedule(scheduleId: ScheduleId) = {
    val now = Instant.now(clock)
    val operation = context.table
      .given(
        Condition('status         -> "Pending")
          or AndCondition('status -> "Orchestrating", 'orchestrationExpiry < now.toEpochMilli)
      )
      .update('scheduleId -> scheduleId,
              set('status -> (ScheduleStatus.Cancelled: ScheduleStatus))
                and append('history, Change(now, "Cancelled")))

    Scanamo.exec(context.db)(operation) match {
      case Right(schedule)          => Some(Right(schedule))
      case Left(ConditionNotMet(_)) => None
      case Left(error) =>
        log.warn(s"Problem cancelling schedule: $scheduleId", error)
        Some(Left(ErrorDetails(s"Failed to remove schedule from dynamo: $scheduleId", OrchestrationError)))
    }
  }
}
