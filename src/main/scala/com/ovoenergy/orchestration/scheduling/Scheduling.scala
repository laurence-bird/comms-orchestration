package com.ovoenergy.orchestration.scheduling

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time._
import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo._
import com.gu.scanamo.error.ConditionNotMet
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import com.ovoenergy.comms.model.{CommManifest, CommType, Metadata, Triggered}
import com.ovoenergy.orchestration.scheduling.Scheduling.ScheduleStatus._
import org.quartz.impl.StdSchedulerFactory
import org.quartz._
import org.quartz.JobBuilder._
import org.quartz.TriggerBuilder._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Scheduling {

  private val log = LoggerFactory.getLogger(getClass)

  sealed trait ScheduleStatus
  object ScheduleStatus {
    case object Pending extends ScheduleStatus
    case object Orchestrating extends ScheduleStatus
    case object Complete extends ScheduleStatus
    case object Failed extends ScheduleStatus
    case object Cancelled extends ScheduleStatus
  }

  case class Change(timestamp: OffsetDateTime, operation: String)

  case class Schedule(
                       scheduleId: UUID,
                       triggered: Triggered,
                       deliverAt: OffsetDateTime,
                       status: ScheduleStatus,
                       history: Seq[Change],
                       orchestrationExpiry_unique: String
                     )

  private val fixedLengthIsoDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  private def format(offsetDateTime: OffsetDateTime) = offsetDateTime.atZoneSameInstant(ZoneOffset.UTC).format(fixedLengthIsoDateTimeFormat)

  implicit val uuidDynamoFormat = DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val offsetDateTimeDynamoFormat = DynamoFormat.coercedXmap[OffsetDateTime, String, DateTimeParseException](OffsetDateTime.parse)(format)

  implicit val scheduleStatusDynamoFormat = DynamoFormat.coercedXmap[ScheduleStatus, String, MatchError]{
    case "Pending" => Pending
    case "Orchestrating" => Orchestrating
    case "Complete" => Complete
    case "Failed" => Failed
    case "Cancelled" => Cancelled
  }{
    case Pending => "Pending"
    case Orchestrating => "Orchestrating"
    case Complete => "Complete"
    case Failed => "Failed"
    case Cancelled => "Cancelled"
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

  private val table = Table[Schedule]("scheduling-spike")

  private val quartzScheduler = StdSchedulerFactory.getDefaultScheduler

  def init(dynamo: AmazonDynamoDB, orchestrator: Triggered => Future[_], onFailure: (String, Triggered) => Future[_]): Unit = {
    quartzScheduler.start()

    insertDummyRecords(dynamo)

    val createScheduledJob = scheduleJob(jobFunction(dynamo, orchestrator, onFailure)) _
    val now = format(OffsetDateTime.now)

    val pending = Scanamo.exec(dynamo)(table.index("status-index").query('status -> (Pending: ScheduleStatus)))
    for {
      either <- pending
      x <- either.right
    } {
      log.info(s"Scheduling delivery of pending schedule ${x.scheduleId} at ${x.deliverAt})")
      createScheduledJob(x.scheduleId, x.deliverAt)
    }

    val expired = Scanamo.exec(dynamo)(table.index("status-index").query('status -> (Orchestrating: ScheduleStatus) and 'orchestrationExpiry_unique < now))
    for {
      either <- expired
      x <- either.right
    } {
      log.info(s"Scheduling delivery of expired schedule ${x.scheduleId} at ${x.deliverAt})")
      createScheduledJob(x.scheduleId, x.deliverAt)
    }
  }

  class OrchestrationJob extends Job {
    private def getAs[T](key: String)(implicit jobDataMap: JobDataMap): T = jobDataMap.get(key).asInstanceOf[T]

    override def execute(context: JobExecutionContext): Unit = {
      implicit val jobDataMap = context.getJobDetail.getJobDataMap

      val scheduleId = getAs[UUID]("scheduleId")
      val function = getAs[UUID => _]("function")

      log.info(s"Executing scheduled orchestration of $scheduleId")
      function.apply(scheduleId)
    }
  }

  private def jobFunction(dynamo: AmazonDynamoDB, orchestrator: Triggered => Future[_], onFailure: (String, Triggered) => Future[_]) = (scheduleId: UUID) => {
    log.info(s"Running scheduled job for schedule $scheduleId")

    val now = OffsetDateTime.now

    /*
    This monstrosity works, but then I worked out how to write the same thing with Scanamo.
    (Because the DSL overloads the word "and" for various things, type inference needed a bit of help)
     */
    //    val updateRequest = new UpdateItemRequest()
    //      .withTableName(table.name)
    //      .addKeyEntry("scheduleId", uuidDynamoFormat.write(scheduleId))
    //      .withReturnValues(ReturnValue.ALL_NEW)
    //      .withConditionExpression("#status = :pending or (#status = :orchestrating and #expiry < :now)")
    //      .withUpdateExpression("SET #status = :orchestrating, #expiry = :newExpiry, #history = list_append(#history, :change)")
    //      .addExpressionAttributeNamesEntry("#status", "status")
    //      .addExpressionAttributeNamesEntry("#expiry", "orchestrationExpiry_unique")
    //      .addExpressionAttributeNamesEntry("#history", "history")
    //      .addExpressionAttributeValuesEntry(":pending", scheduleStatusDynamoFormat.write(Pending))
    //      .addExpressionAttributeValuesEntry(":orchestrating", scheduleStatusDynamoFormat.write(Orchestrating))
    //      .addExpressionAttributeValuesEntry(":now", DynamoFormat[String].write(format(now)))
    //      .addExpressionAttributeValuesEntry(":newExpiry", DynamoFormat[String].write(s"${format(now.plusMinutes(5))}_$scheduleId"))
    //      .addExpressionAttributeValuesEntry(":change", DynamoFormat.listFormat[Change].write(List(Change(now.toInstant, "Start orchestrating"))))

    val conditionalUpdate = table
      .given(
        Condition('status -> "Pending")
        or AndCondition('status -> "Orchestrating", 'orchestrationExpiry_unique < format(OffsetDateTime.now)))
      .update('scheduleId -> scheduleId,
        set('status -> (Orchestrating: ScheduleStatus))
        and set('orchestrationExpiry_unique, s"${format(now.plusMinutes(5))}_$scheduleId")
        and append('history, Change(now, "Start orchestrating")))

    Scanamo.exec(dynamo)(conditionalUpdate) match {
      case Left(ConditionNotMet(ex)) =>
        log.info(s"Condition not met for update of schedule $scheduleId. Nothing to do. Details: $ex")
      case Left(err) =>
        log.warn(s"Conditional update of schedule $scheduleId failed. Error: $err")
      case Right(schedule) =>
        orchestrator(schedule.triggered) andThen {
          case Success(_) =>
            val update = table
              .update('scheduleId -> scheduleId,
                set('status -> (Complete: ScheduleStatus))
                  and append('history, Change(OffsetDateTime.now(), "Successfully orchestrated")))
            Scanamo.exec(dynamo)(update).right.foreach { _ =>
              log.info(s"Marked schedule $scheduleId as Complete in Dynamo")
            }
          case Failure(error) =>
            onFailure(s"Orchestration failed: ${error.getMessage}", schedule.triggered) andThen {
              case successOrFailure =>
                val update = table
                  .update('scheduleId -> scheduleId,
                    set('status -> (Failed: ScheduleStatus))
                      and append('history, Change(OffsetDateTime.now(), "Orchestration failed")))
                Scanamo.exec(dynamo)(update).right.foreach { _ =>
                  log.info(s"Marked schedule $scheduleId as Failed in Dynamo")
                }
            }
        }
    }
  }

  private def scheduleJob(jobFunction: UUID => _)(scheduleId: UUID, startAt: OffsetDateTime): Unit = {
    val jobDetail = newJob(classOf[OrchestrationJob])
      .withIdentity(new JobKey(scheduleId.toString))
      .usingJobData(buildJobDataMap(scheduleId, jobFunction))
      .build()
    val trigger = newTrigger()
      .withIdentity(new TriggerKey(scheduleId.toString))
      .startAt(Date.from(startAt.toInstant))
      .build()
    quartzScheduler.scheduleJob(jobDetail, trigger)
  }

  private def buildJobDataMap(scheduleId: UUID, jobFunction: UUID => _): JobDataMap = {
    val map = new JobDataMap()
    map.put("scheduleId", scheduleId)
    map.put("function", jobFunction)
    map
  }

  private def insertDummyRecords(dynamo: AmazonDynamoDB): Unit = {
    val now = OffsetDateTime.now()
    val deliverAt1 = OffsetDateTime.now().plusSeconds(30)
    val uuid1 = UUID.randomUUID()
    val schedule1 = Schedule(
      uuid1,
      Triggered(
        Metadata(
          now.toString,
          "triggered1",
          "customer1",
          "trace1",
          CommManifest(CommType.Service, "my-comm", "1.0"),
          "first trigger",
          "some service",
          canary = false,
          None
        ),
        Map.empty,
        Some(deliverAt1.toString)
      ),
      deliverAt1,
      Pending,
      Nil,
      s"${format(now)}_$uuid1"
    )

    val deliverAt2 = OffsetDateTime.now().plusHours(6)
    val uuid2 = UUID.randomUUID()
    val schedule2 = Schedule(
      uuid2,
      Triggered(
        Metadata(
          OffsetDateTime.now().toString,
          "triggered2",
          "customer2",
          "trace2",
          CommManifest(CommType.Service, "my-comm", "1.0"),
          "second trigger",
          "some service",
          canary = false,
          None
        ),
        Map.empty,
        Some(deliverAt2.toString)
      ),
      deliverAt2,
      ScheduleStatus.Pending,
      Nil,
      s"${format(now)}_$uuid2"
    )

    val deliverAt3 = OffsetDateTime.now().minusHours(2)
    val uuid3 = UUID.randomUUID()
    val schedule3 = Schedule(
      uuid3,
      Triggered(
        Metadata(
          OffsetDateTime.now().toString,
          "triggered3",
          "customer3",
          "trace3",
          CommManifest(CommType.Service, "my-comm", "1.0"),
          "third trigger",
          "some service",
          canary = false,
          None
        ),
        Map.empty,
        Some(deliverAt3.toString)
      ),
      deliverAt3,
      ScheduleStatus.Orchestrating,
      Nil,
      s"${format(now.minusHours(1))}_$uuid3"
    )

    val deliverAt4 = OffsetDateTime.now().minusMinutes(3)
    val uuid4 = UUID.randomUUID()
    val schedule4 = Schedule(
      uuid4,
      Triggered(
        Metadata(
          OffsetDateTime.now().toString,
          "triggered4",
          "customer4",
          "trace4",
          CommManifest(CommType.Service, "my-comm", "1.0"),
          "fourth trigger",
          "some service",
          canary = false,
          None
        ),
        Map.empty,
        Some(deliverAt4.toString)
      ),
      deliverAt4,
      ScheduleStatus.Orchestrating,
      Nil,
      s"${format(now.plusMinutes(2))}_$uuid4"
    )
    Scanamo.exec(dynamo)(table.putAll(Set(schedule1, schedule2, schedule3, schedule4)))
  }

}
