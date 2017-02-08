package com.ovoenergy.orchestration.processes

import java.time.{Clock, Instant, ZoneId}

import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.ScheduleStatus.Pending
import com.ovoenergy.orchestration.scheduling._
import com.ovoenergy.orchestration.util.TestUtil
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class SchedulerSpec extends FlatSpec with Matchers with OneInstancePerTest {

  val now = Instant.now()
  val clock = Clock.fixed(now, ZoneId.of("UTC"))

  var storedSchedule = Option.empty[Schedule]
  val storeSchedule = (schedule: Schedule) => storedSchedule = Some(schedule)

  var scheduledId = Option.empty[ScheduleId]
  var scheduledInstant = Option.empty[Instant]
  val scheduleTask = (scheduleId: ScheduleId, instant: Instant) => {
    scheduledId = Some(scheduleId)
    scheduledInstant = Some(instant)
  }

  behavior of "Scheduler"

  it should "persist and schedule an immediate comm" in {
    val triggered = TestUtil.triggered.copy(deliverAt = None)
    Scheduler(storeSchedule, scheduleTask, clock)(triggered) shouldBe Right(())

    //Side effects
    storedSchedule.get.triggered shouldBe triggered
    storedSchedule.get.commName shouldBe triggered.metadata.commManifest.name
    storedSchedule.get.customerId shouldBe triggered.metadata.customerId
    storedSchedule.get.deliverAt shouldBe now
    storedSchedule.get.status shouldBe Pending
    scheduledId.isDefined shouldBe true
    scheduledInstant shouldBe Some(now)
  }

  it should "persist and schedule a future comm" in {
    val triggered = TestUtil.triggered.copy(deliverAt = Some("2036-01-01T12:34:44.000Z"))
    Scheduler(storeSchedule, scheduleTask, clock)(triggered) shouldBe Right(())

    //Side effects
    storedSchedule.get.triggered shouldBe triggered
    storedSchedule.get.commName shouldBe triggered.metadata.commManifest.name
    storedSchedule.get.customerId shouldBe triggered.metadata.customerId
    storedSchedule.get.deliverAt shouldBe Instant.ofEpochMilli(2082803684000l)
    storedSchedule.get.status shouldBe Pending
    scheduledId.isDefined shouldBe true
    scheduledInstant shouldBe Some(Instant.ofEpochMilli(2082803684000l))
  }

  it should "handle exceptions when persisting scheduled comm" in {
    val storeSchedule = (schedule: Schedule) => throw new RuntimeException("Some error")
    Scheduler(storeSchedule, scheduleTask, clock)(TestUtil.triggered) shouldBe Left(ErrorDetails("Error scheduling comm: Some error", OrchestrationError))
  }

  it should "handle exceptions when scheduling task" in {
    val scheduleTask = (scheduleId: ScheduleId, instant: Instant) => throw new RuntimeException("Some error")
    Scheduler(storeSchedule, scheduleTask, clock)(TestUtil.triggered) shouldBe Left(ErrorDetails("Error scheduling comm: Some error", OrchestrationError))
  }
}
