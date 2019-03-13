package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Clock, DateTimeException, Instant}
import java.util.{UUID, Map => JMap}

import scala.concurrent.duration._

import cats.effect.Async
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, DynamoReadError, TypeCoercionError}
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.aws.AwsProvider.DbClients
import com.ovoenergy.orchestration.logging.LoggingWithMDC
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
import scala.concurrent.ExecutionContext

object DynamoPersistence extends DynamoFormats with LoggingWithMDC {

  def generateScheduleId(): ScheduleId = {
    UUID.randomUUID().toString
  }

  case class Context(db: AmazonDynamoDBAsync, tableName: String) {
    val table = Table[Schedule](tableName)
  }
}

class DynamoPersistence(orchestrationExpiry: FiniteDuration, context: Context, clock: Clock = Clock.systemUTC())(
    implicit ec: ExecutionContext)
    extends Persistence.Orchestration
    with Persistence.Listing {

  import DynamoPersistence._

  private val log = LoggerFactory.getLogger("Persistence")

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
          and set('orchestrationExpiry, now.plusSeconds(orchestrationExpiry.toSeconds).toEpochMilli)
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
}
