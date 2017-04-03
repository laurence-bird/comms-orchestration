package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Clock, Instant, ZoneId}

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Successful}
import com.ovoenergy.orchestration.scheduling._
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.{Context}
import com.ovoenergy.orchestration.util.{ArbGenerator, LocalDynamoDB}
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

import scala.collection.mutable

class DynamoPersistenceSpec extends FlatSpec with Matchers with ArbGenerator {

  val now       = Instant.now()
  val clock     = Clock.fixed(now, ZoneId.of("UTC"))
  val tableName = "scheduling"
  val client    = LocalDynamoDB.client()
  val secondaryIndices = Seq(
    SecondaryIndexData("customerId-commName-index", Seq('customerId    -> S, 'commName            -> S)),
    SecondaryIndexData("status-orchestrationExpiry-index", Seq('status -> S, 'orchestrationExpiry -> N))
  )
  val persistence = new DynamoPersistence(5, Context(client, tableName), clock)

  behavior of "Persistence"

  it should "store scheduled comm" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val scheduledComm = generate[Schedule].copy(status = ScheduleStatus.Pending)
      persistence.storeSchedule(scheduledComm)
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
      persistence.storeSchedule(completedSchedule)
      persistence.storeSchedule(pendingSchedule)
      persistence.storeSchedule(expiredOrchestratingSchedule)
      persistence.storeSchedule(inProgressOrchestratingSchedule)
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
      persistence.storeSchedule(completedSchedule)
      persistence.storeSchedule(pendingSchedule)
      persistence.storeSchedule(expiredOrchestratingSchedule)
      persistence.storeSchedule(inProgressOrchestratingSchedule)
      val pending = persistence.listExpiredSchedules()
      pending shouldBe List(expiredOrchestratingSchedule)
    }
  }

  it should "correctly mark a schedule as orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule].copy(status = ScheduleStatus.Pending, history = Seq())
      persistence.storeSchedule(schedule)
      val result                = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = persistence.retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe Successful(
        schedule.copy(status = ScheduleStatus.Orchestrating,
                      history = Seq(Change(now, "Start orchestrating")),
                      orchestrationExpiry = now.plusSeconds(60 * 5)))
      orchestratingSchedule.get.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.get.orchestrationExpiry shouldBe now.plusSeconds(60 * 5)
    }
  }

  it should "not mark a schedule as orchestrating that is already orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val orchestrationExpiry = now.plusSeconds(60 * 2)
      val schedule =
        generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = orchestrationExpiry)
      persistence.storeSchedule(schedule)
      val result                = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = persistence.retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe AlreadyBeingOrchestrated
      orchestratingSchedule.get.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.get.orchestrationExpiry shouldBe orchestrationExpiry
    }
  }

  it should "cancel pending and expired orchestrating schedules" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val commName   = "some-comm"
      val customerId = "23141141241"
      val completedSchedule =
        generate[Schedule].copy(customerId = customerId, commName = commName, status = ScheduleStatus.Complete)
      val pendingSchedule = generate[Schedule].copy(history = List(),
                                                    customerId = customerId,
                                                    commName = commName,
                                                    status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(history = List(),
                                                                 customerId = customerId,
                                                                 commName = commName,
                                                                 status = ScheduleStatus.Orchestrating,
                                                                 orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(customerId = customerId,
                                                                    commName = commName,
                                                                    status = ScheduleStatus.Orchestrating,
                                                                    orchestrationExpiry = now.plusSeconds(60 * 10))
      persistence.storeSchedule(completedSchedule)
      persistence.storeSchedule(pendingSchedule)
      persistence.storeSchedule(expiredOrchestratingSchedule)
      persistence.storeSchedule(inProgressOrchestratingSchedule)

      val cancelled = persistence.cancelSchedules(customerId, commName)

      persistence.retrieveSchedule(completedSchedule.scheduleId.toString).get shouldBe completedSchedule
      persistence
        .retrieveSchedule(inProgressOrchestratingSchedule.scheduleId.toString)
        .get shouldBe inProgressOrchestratingSchedule

      val cancelledPendingSchedule =
        pendingSchedule.copy(status = ScheduleStatus.Cancelled, history = List(Change(now, "Cancelled")))
      persistence.retrieveSchedule(pendingSchedule.scheduleId.toString).get shouldBe cancelledPendingSchedule

      val cancelledExpiredOrchestratingSchedule =
        expiredOrchestratingSchedule.copy(status = ScheduleStatus.Cancelled, history = List(Change(now, "Cancelled")))
      persistence
        .retrieveSchedule(expiredOrchestratingSchedule.scheduleId.toString)
        .get shouldBe cancelledExpiredOrchestratingSchedule

      cancelled.size shouldBe 2
      cancelled should contain(Right(cancelledPendingSchedule))
      cancelled should contain(Right(cancelledExpiredOrchestratingSchedule))
    }
  }

  //Probably not worth running all the time as pretty slow (2 minutes)
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
                                                      customerId = customerId,
                                                      commName = commName,
                                                      status = ScheduleStatus.Pending)
        persistence.storeSchedule(pendingSchedule)
      })
      val returned = persistence.cancelSchedules(customerId, commName)
      returned.size shouldBe howMany
      returned.map(_.right.get.scheduleId).sorted shouldBe ids.toList.sorted
    }
  }

  it should "mark schedules as failed" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule]
      persistence.storeSchedule(schedule)
      persistence.setScheduleAsFailed(schedule.scheduleId.toString, "Invalid profile")

      val result = persistence.retrieveSchedule(schedule.scheduleId.toString).get
      result.status shouldBe ScheduleStatus.Failed
      result.history should contain(Change(now, "Failed - Invalid profile"))
    }
  }

  it should "mark schedules as complete" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule]
      persistence.storeSchedule(schedule)
      persistence.setScheduleAsComplete(schedule.scheduleId.toString)

      val result = persistence.retrieveSchedule(schedule.scheduleId.toString).get
      result.status shouldBe ScheduleStatus.Complete
      result.history should contain(Change(now, "Orchestration complete"))
    }
  }
}
