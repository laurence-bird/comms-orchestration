package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Clock, DateTimeException, Instant}
import java.util.{UUID, Map => JMap}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, TypeCoercionError}
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model.{CommType, TemplateData}
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Failed, SetAsOrchestratingResult, Successful}
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

  implicit val uuidDynamoFormat = DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val instantDynamoFormat = DynamoFormat.coercedXmap[Instant, Long, DateTimeException](Instant.ofEpochMilli)(_.toEpochMilli)

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

class DynamoPersistence(orchestrationExpiryMinutes: Int, context: Context, clock: Clock = Clock.systemUTC())
  extends Persistence.Orchestration
  with Persistence.Listing {

  import DynamoPersistence._

  private val log = LoggerFactory.getLogger("Persistence")

  def storeSchedule(commSchedule: Schedule): Unit = {
    Scanamo.exec(context.db)(context.table.put(commSchedule))
    log.debug(s"Persisted comm schedule: $commSchedule")
  }

  def retrieveSchedule(scheduleId: ScheduleId): Option[Schedule] = {
    Scanamo.get[Schedule](context.db)(context.table.name)('scheduleId -> scheduleId) match {
      case Some(Left(error))               =>
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
      case Right(schedule)          => Successful(schedule)
    }
  }

  def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
    val now = Instant.now(clock)
    val result = Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Failed: ScheduleStatus))
        and append('history, Change(now, s"Failed - $reason"))
    ))
    result match {
      case Left(error) => log.warn(s"Error marking schedule as Failed $error")
      case Right(_)    => ()
    }
  }

  def setScheduleAsComplete(scheduleId: ScheduleId): Unit = {
    val now = Instant.now(clock)
    val result = Scanamo.exec(context.db)(context.table.update('scheduleId -> scheduleId,
      set('status -> (ScheduleStatus.Complete: ScheduleStatus))
        and append('history, Change(now, "Orchestration complete"))
    ))
    result match {
      case Left(error) => log.warn(s"Error marking schedule as Complete $error")
      case Right(_)    => ()
    }
  }

  def listPendingSchedules(): List[Schedule] = {
    val pending = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Pending: ScheduleStatus)))

    pending flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving pending schedule", e)
        None
    }
  }

  def listExpiredSchedules(): List[Schedule] = {
    val now = Instant.now(clock).toEpochMilli
    val expired = Scanamo.exec(context.db)(context.table.index("status-orchestrationExpiry-index").query('status -> (ScheduleStatus.Orchestrating: ScheduleStatus) and 'orchestrationExpiry < now))

    expired flatMap {
      case Right(schedule) => Some(schedule)
      case Left(e) =>
        log.warn("Problem retrieving expired schedule", e)
        None
    }
  }

  def cancelSchedules(customerId: String, commName: String): Seq[Schedule] = {
    val now = Instant.now(clock)
    val db = context.db
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
      .addExpressionAttributeValuesEntry(":orchestrating", scheduleStatusDynamoFormat.write(ScheduleStatus.Orchestrating))
      .addExpressionAttributeValuesEntry(":now", instantDynamoFormat.write(now))
      .withFilterExpression("#status = :pending or (#status = :orchestrating and #expiry < :now)")

    @tailrec
    def pageQuery(key: JMap[String,AttributeValue], currentItems: Vector[JMap[String,AttributeValue]] ): Vector[JMap[String,AttributeValue]] = {
      query.setExclusiveStartKey(key)
      val queryResult = db.query(query)
      val evaluationKey = queryResult.getLastEvaluatedKey
      val items = currentItems ++ queryResult.getItems.asScala.toVector
      if (evaluationKey == null) items
      else pageQuery(evaluationKey, items)
    }

    pageQuery(null, Vector.empty[JMap[String,AttributeValue]])
      .map(item => {
        DynamoFormat[Schedule].read(new AttributeValue().withM(item))
      })
      .flatMap {
        case Right(schedule) =>
          attemptToCancelSchedule(schedule.scheduleId)
        case Left(e) =>
          log.warn("Problem retrieving pending schedule", e)
          None
      }
  }

  private def attemptToCancelSchedule(scheduleId: ScheduleId): Option[Schedule] = {
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