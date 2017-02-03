package com.ovoenergy.orchestration.scheduling

import java.time.{Clock, ZoneId, ZonedDateTime}

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Successful}
import org.scalatest.{FlatSpec, Matchers}
import com.ovoenergy.orchestration.util.LocalDynamoDB
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalacheck.Shapeless._

class PersistenceSpec extends FlatSpec with Matchers with ArbGenerator {

  val now = ZonedDateTime.now(ZoneId.of("UTC"))
  val clock = Clock.fixed(now.toInstant, ZoneId.of("UTC"))
  val tableName = "scheduling"
  val client = LocalDynamoDB.client()
  val secondaryIndices = Seq(
    SecondaryIndexData("customerId-commName-index", Seq('customerId -> S, 'commName -> S)),
    SecondaryIndexData("status-orchestrationExpiry-index", Seq('status -> S, 'orchestrationExpiry -> N))
  )
  val persistence = new Persistence(Context(client, tableName), clock)

  behavior of "Persistence"

  it should "store scheduled comm" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val scheduledComm = generate[Schedule].copy(status = ScheduleStatus.Pending)
      persistence.storeSchedule(scheduledComm)
      val pending = persistence.retrievePendingSchedules()
      pending.size shouldBe 1
      pending.head shouldBe scheduledComm
    }
  }

  it should "return pending schedules including expired orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val completedSchedule = generate[Schedule].copy(status = ScheduleStatus.Complete)
      val pendingSchedule = generate[Schedule].copy(status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = ZonedDateTime.now(ZoneId.of("UTC")).minusMinutes(30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = ZonedDateTime.now(ZoneId.of("UTC")).plusMinutes(10))
      persistence.storeSchedule(completedSchedule)
      persistence.storeSchedule(pendingSchedule)
      persistence.storeSchedule(expiredOrchestratingSchedule)
      persistence.storeSchedule(inProgressOrchestratingSchedule)
      val pending = persistence.retrievePendingSchedules()
      pending.size shouldBe 2
      pending should (contain(pendingSchedule) and contain(expiredOrchestratingSchedule))
    }
  }

  it should "correctly mark a schedule as orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val schedule = generate[Schedule].copy(status = ScheduleStatus.Pending)
      persistence.storeSchedule(schedule)
      val result = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = persistence.retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe Successful
      orchestratingSchedule.get.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.get.orchestrationExpiry shouldBe now.plusMinutes(5)
    }
  }

  it should "not mark a schedule as orchestrating that is already orchestrating" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val orchestrationExpiry = now.plusMinutes(2)
      val schedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = orchestrationExpiry)
      persistence.storeSchedule(schedule)
      val result = persistence.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString)
      val orchestratingSchedule = persistence.retrieveSchedule(schedule.scheduleId.toString)
      result shouldBe AlreadyBeingOrchestrated
      orchestratingSchedule.get.status shouldBe ScheduleStatus.Orchestrating
      orchestratingSchedule.get.orchestrationExpiry shouldBe orchestrationExpiry
    }
  }

  it should "cancel pending and expired orchestrating schedules" in {
    LocalDynamoDB.withTableWithSecondaryIndex(client, tableName)(Seq('scheduleId -> S))(secondaryIndices) {
      val commName = "some-comm"
      val customerId = "23141141241"
      val completedSchedule = generate[Schedule].copy(customerId = customerId, commName = commName, status = ScheduleStatus.Complete)
      val pendingSchedule = generate[Schedule].copy(customerId = customerId, commName = commName, status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(customerId = customerId, commName = commName, status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.minusMinutes(30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(customerId = customerId, commName = commName, status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.plusMinutes(10))
      persistence.storeSchedule(completedSchedule)
      persistence.storeSchedule(pendingSchedule)
      persistence.storeSchedule(expiredOrchestratingSchedule)
      persistence.storeSchedule(inProgressOrchestratingSchedule)

      persistence.cancelSchedules(customerId, commName)

      persistence.retrieveSchedule(completedSchedule.scheduleId.toString).get.status shouldBe ScheduleStatus.Complete
      persistence.retrieveSchedule(pendingSchedule.scheduleId.toString).get.status shouldBe ScheduleStatus.Cancelled
      persistence.retrieveSchedule(expiredOrchestratingSchedule.scheduleId.toString).get.status shouldBe ScheduleStatus.Cancelled
      persistence.retrieveSchedule(inProgressOrchestratingSchedule.scheduleId.toString).get.status shouldBe ScheduleStatus.Orchestrating
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
