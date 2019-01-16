package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Clock, Instant, ZoneId}

import cats.effect.IO
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.aws.AwsProvider.DbClients
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Successful}
import com.ovoenergy.orchestration.scheduling._
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.Context
import com.ovoenergy.orchestration.util.{ArbGenerator, ArbInstances, LocalDynamoDB}
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData
import org.scalacheck.Shapeless._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class DynamoPersistenceSpec extends FlatSpec with Matchers with ArbInstances {

  val now         = Instant.now()
  val clock       = Clock.fixed(now, ZoneId.of("UTC"))
  val tableName   = "scheduling"
  val asyncClient = LocalDynamoDB.asyncClient()
  val client      = LocalDynamoDB.client()
  val secondaryIndices = Seq(
    SecondaryIndexData("customerId-templateId-index", Seq('customerId  -> S, 'templateId          -> S)),
    SecondaryIndexData("status-orchestrationExpiry-index", Seq('status -> S, 'orchestrationExpiry -> N))
  )
  val context          = Context(DbClients(asyncClient, client), tableName)
  val persistence      = new DynamoPersistence(5, context, clock)
  val asyncPersistence = new AsyncPersistence(5, context, clock)

  def storeSchedule(schedule: Schedule) = {
    asyncPersistence
      .storeSchedule[IO]
      .apply(schedule)
      .unsafeRunSync()
  }

  def retrieveSchedule(scheduleId: ScheduleId): Schedule = {
    asyncPersistence
      .retrieveSchedule[IO](scheduleId)
      .unsafeRunSync()
      .getOrElse(fail(s"Schedule doesn't exist with id: ${scheduleId}"))
  }

  behavior of "Persistence"

  it should "store scheduled comm" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val scheduledComm = generate[Schedule].copy(status = ScheduleStatus.Pending)
      storeSchedule(scheduledComm)
      val pending = persistence.listPendingSchedules()
      pending.size shouldBe 1
      pending.head shouldBe scheduledComm
    }
  }

  it should "return pending schedules" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val completedSchedule = generate[Schedule].copy(status = ScheduleStatus.Complete)
      val pendingSchedule   = generate[Schedule].copy(status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.plusSeconds(60 * 10))

      storeSchedule(completedSchedule)
      storeSchedule(pendingSchedule)
      storeSchedule(expiredOrchestratingSchedule)
      storeSchedule(inProgressOrchestratingSchedule)
      val pending = persistence.listPendingSchedules()
      pending shouldBe List(pendingSchedule)
    }
  }

  it should "return expired orchestrating schedules" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val completedSchedule = generate[Schedule].copy(status = ScheduleStatus.Complete)
      val pendingSchedule   = generate[Schedule].copy(status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.plusSeconds(60 * 10))
      storeSchedule(completedSchedule)
      storeSchedule(pendingSchedule)
      storeSchedule(expiredOrchestratingSchedule)
      storeSchedule(inProgressOrchestratingSchedule)
      val pending = persistence.listExpiredSchedules()
      pending shouldBe List(expiredOrchestratingSchedule)
    }
  }

  it should "correctly mark a schedule as orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule].copy(status = ScheduleStatus.Pending, history = Seq())
      storeSchedule(schedule)
      val result                = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe Successful(
        schedule.copy(status = ScheduleStatus.Orchestrating,
                      history = Seq(Change(now, "Start orchestrating")),
                      orchestrationExpiry = now.plusSeconds(60 * 5)))
      orchestratingSchedule.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.orchestrationExpiry shouldBe now.plusSeconds(60 * 5)
    }
  }

  it should "not mark a schedule as orchestrating that is already orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val orchestrationExpiry = now.plusSeconds(60 * 2)
      val schedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = orchestrationExpiry)
      storeSchedule(schedule)
      val result                = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe AlreadyBeingOrchestrated
      orchestratingSchedule.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.orchestrationExpiry shouldBe orchestrationExpiry
    }
  }

  it should "cancel pending and expired orchestrating schedules" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val templateId = Hash("some-comm")
      val customerId = "23141141241"
      val completedSchedule =
        generate[Schedule].copy(customerId = Some(customerId),
                                templateId = templateId,
                                status = ScheduleStatus.Complete)
      val pendingSchedule = generate[Schedule].copy(history = List(),
                                                    customerId = Some(customerId),
                                                    templateId = templateId,
                                                    status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(history = List(),
                                                                 customerId = Some(customerId),
                                                                 templateId = templateId,
                                                                 status = ScheduleStatus.Orchestrating,
                                                                 orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(customerId = Some(customerId),
                                                                    templateId = templateId,
                                                                    status = ScheduleStatus.Orchestrating,
                                                                    orchestrationExpiry = now.plusSeconds(60 * 10))
      storeSchedule(completedSchedule)
      storeSchedule(pendingSchedule)
      storeSchedule(expiredOrchestratingSchedule)
      storeSchedule(inProgressOrchestratingSchedule)

      val cancelled = persistence.cancelSchedules(customerId, templateId)

      retrieveSchedule(completedSchedule.scheduleId.toString) shouldBe completedSchedule
      retrieveSchedule(inProgressOrchestratingSchedule.scheduleId.toString) shouldBe inProgressOrchestratingSchedule

      val cancelledPendingSchedule =
        pendingSchedule.copy(status = ScheduleStatus.Cancelled, history = List(Change(now, "Cancelled")))
      retrieveSchedule(pendingSchedule.scheduleId.toString) shouldBe cancelledPendingSchedule

      val cancelledExpiredOrchestratingSchedule =
        expiredOrchestratingSchedule.copy(status = ScheduleStatus.Cancelled, history = List(Change(now, "Cancelled")))

      retrieveSchedule(expiredOrchestratingSchedule.scheduleId.toString) shouldBe cancelledExpiredOrchestratingSchedule

      cancelled.size shouldBe 2
      cancelled should contain(Right(cancelledPendingSchedule))
      cancelled should contain(Right(cancelledExpiredOrchestratingSchedule))
    }
  }

  /* //Probably not worth running all the time as pretty slow (2 minutes)
  ignore should "handle paging when retrieving schedules to cancel" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val commName   = "some-comm"
      val customerId = "23141141241"
      val howMany    = 10000
      val ids        = mutable.MutableList[ScheduleId]()
      (1 to howMany).foreach(_ => {
        val id = DynamoPersistence.generateScheduleId()
        ids += id
        val pendingSchedule = generate[Schedule].copy(scheduleId = id,
                                                      customerId = Some(customerId),
                                                      commName = commName,
                                                      status = ScheduleStatus.Pending)
        persistence.storeSchedule(pendingSchedule)
      })
      val returned = persistence.cancelSchedules(customerId, commName)
      returned.size shouldBe howMany
      returned.map(_.right.get.scheduleId).sorted shouldBe ids.toList.sorted
    }
  }*/

  it should "mark schedules as failed" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule]
      storeSchedule(schedule)
      persistence.setScheduleAsFailed(schedule.scheduleId.toString, "Invalid profile")

      val result = retrieveSchedule(schedule.scheduleId.toString)
      result.status shouldBe ScheduleStatus.Failed
      result.history should contain(Change(now, "Failed - Invalid profile"))
    }
  }

  it should "mark schedules as complete" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule]
      storeSchedule(schedule)
      persistence.setScheduleAsComplete(schedule.scheduleId.toString)

      val result = retrieveSchedule(schedule.scheduleId.toString)
      result.status shouldBe ScheduleStatus.Complete
      result.history should contain(Change(now, "Orchestration complete"))
    }
  }
}
