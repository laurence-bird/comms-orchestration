package com.ovoenergy.orchestration.processes

import java.time.{Clock, Instant, ZoneId}

import com.ovoenergy.comms.model.{CancellationRequested, ErrorCode, Metadata}
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.Scheduler.{CommName, CustomerId}
import com.ovoenergy.orchestration.scheduling.ScheduleStatus.Pending
import com.ovoenergy.orchestration.scheduling._
import com.ovoenergy.orchestration.util.{ArbGenerator, TestUtil}
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._

class SchedulerSpec extends FlatSpec with Matchers with OneInstancePerTest with ArbGenerator{

  val now = Instant.now()
  val clock = Clock.fixed(now, ZoneId.of("UTC"))

  var storedSchedule = Option.empty[Schedule]
  val storeSchedule = (schedule: Schedule) => storedSchedule = Some(schedule)

  var scheduledId = Option.empty[ScheduleId]
  var scheduledInstant = Option.empty[Instant]
  val registerTask = (scheduleId: ScheduleId, instant: Instant) => {
    scheduledId = Some(scheduleId)
    scheduledInstant = Some(instant)
    true
  }

  behavior of "Scheduler"

  it should "persist and schedule an immediate comm" in {
    val triggered = TestUtil.triggered.copy(deliverAt = None)
    Scheduler.scheduleComm(storeSchedule, registerTask, clock)(triggered) shouldBe Right(true)

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
    Scheduler.scheduleComm(storeSchedule, registerTask, clock)(triggered) shouldBe Right(true)

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
    Scheduler.scheduleComm(storeSchedule, registerTask, clock)(TestUtil.triggered) shouldBe Left(ErrorDetails("Failed to schedule comm", OrchestrationError))
  }

  it should "handle exceptions when scheduling task" in {
    val scheduleTask = (scheduleId: ScheduleId, instant: Instant) => throw new RuntimeException("Some error")
    Scheduler.scheduleComm(storeSchedule, scheduleTask, clock)(TestUtil.triggered) shouldBe Left(ErrorDetails("Failed to schedule comm", OrchestrationError))
  }

  it should "return successful result if a cancellationRequest is successful" in {
    val cancellationRequested = generate[CancellationRequested]
    val schedules = Seq(Right(generate[Schedule]), Right(generate[Schedule]))
    val removeFromPersistence = (customerId: CustomerId, commName: CommName) => schedules
    val removeSchedule = (scheduledId: ScheduleId) => true
    Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested) shouldBe
      schedules.map(_.right.map(_.triggered.metadata))
  }

  it should "capture an appropriate error if removing the schedule from pesistent storage fails" in {
    val cancellationRequested = generate[CancellationRequested]
    val expectedError = List(Left(ErrorDetails("Failed to remove from persistence", ErrorCode.OrchestrationError)))

    val removeFromPersistence = (customerId: CustomerId, commName: CommName) => expectedError
    val removeSchedule = (scheduledId: ScheduleId) => true
    Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested) shouldBe expectedError
  }

  it should "capture an appropriate error if removing the schedule from memory fails for a single record" in {
    val cancellationRequested = generate[CancellationRequested]
    val successfulSchedule = generate[Schedule]
    val failedSchedule = generate[Schedule]
    val schedules = Seq(Right(successfulSchedule), Right(failedSchedule))

    val removeFromPersistence = (customerId: CustomerId, commName: CommName) => schedules
    val removeSchedule = (scheduledId: ScheduleId) => {
      if(scheduledId == failedSchedule.scheduleId) false
      else true
    }

    val expectedError= Left(ErrorDetails(s"Failed to remove ${failedSchedule.scheduleId} schedule(s) from memory", OrchestrationError))
    val result = Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested)
    result should contain theSameElementsAs List(Right(successfulSchedule.triggered.metadata), expectedError)
  }
}
