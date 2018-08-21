package com.ovoenergy.comms.orchestration.processes

import java.time.{Clock, Instant, ZoneId}

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.comms.orchestration.processes.Scheduler.{TemplateId, CustomerId}
import com.ovoenergy.comms.orchestration.scheduling.ScheduleStatus.Pending
import com.ovoenergy.comms.orchestration.scheduling._
import com.ovoenergy.comms.orchestration.util.{ArbGenerator, TestUtil}
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._

class SchedulerSpec extends FlatSpec with Matchers with OneInstancePerTest with ArbGenerator {

  val now   = Instant.now()
  val clock = Clock.fixed(now, ZoneId.of("UTC"))

  var storedSchedule = Option.empty[Schedule]
  val storeSchedule: Schedule => IO[Option[Schedule]] = (schedule: Schedule) => {
    storedSchedule = Some(schedule)
    IO.pure(storedSchedule)
  }

  var scheduledId      = Option.empty[ScheduleId]
  var scheduledInstant = Option.empty[Instant]
  val registerTask = (scheduleId: ScheduleId, instant: Instant) => {
    scheduledId = Some(scheduleId)
    scheduledInstant = Some(instant)
    true
  }

  behavior of "Scheduler"

  it should "persist and schedule an immediate comm" in {
    val triggered = TestUtil.customerTriggeredV4.copy(deliverAt = None)
    val result    = Scheduler.scheduleComm[IO](storeSchedule, registerTask, clock).apply(triggered)

    result.unsafeRunSync() shouldBe Right(true)

    //Side effects
    storedSchedule.get.triggeredV4.get shouldBe triggered
    storedSchedule.get.templateId shouldBe triggered.metadata.templateManifest.id
    storedSchedule.get.customerId shouldBe Some(TestUtil.customerId)
    storedSchedule.get.deliverAt shouldBe now
    storedSchedule.get.status shouldBe Pending
    scheduledId.isDefined shouldBe true
    scheduledInstant shouldBe Some(now)
  }

  it should "persist and schedule a future comm" in {
    val triggered = TestUtil.customerTriggeredV4.copy(deliverAt = Some(Instant.parse("2036-01-01T12:34:44.000Z")))
    Scheduler.scheduleComm(storeSchedule, registerTask, clock).apply(triggered).unsafeRunSync() shouldBe Right(true)

    //Side effects
    storedSchedule.get.triggeredV4.get shouldBe triggered
    storedSchedule.get.templateId shouldBe triggered.metadata.templateManifest.id
    storedSchedule.get.customerId shouldBe Some(TestUtil.customerId)
    storedSchedule.get.deliverAt shouldBe Instant.ofEpochMilli(2082803684000l)
    storedSchedule.get.status shouldBe Pending
    scheduledId.isDefined shouldBe true
    scheduledInstant shouldBe Some(Instant.ofEpochMilli(2082803684000l))
  }

  it should "handle exceptions when persisting scheduled comm" in {
    val storeSchedule = (schedule: Schedule) => IO.raiseError(new RuntimeException("Failed to schedule comm"))
    val result        = Scheduler.scheduleComm[IO](storeSchedule, registerTask, clock).apply(TestUtil.customerTriggeredV4)

    result.unsafeRunSync() shouldBe Left(ErrorDetails("Failed to schedule comm", OrchestrationError))
  }

  it should "handle exceptions when scheduling task" in {
    val scheduleTask = (scheduleId: ScheduleId, instant: Instant) => throw new RuntimeException("Some error")
    val result =
      Scheduler.scheduleComm(storeSchedule, scheduleTask, clock).apply(TestUtil.customerTriggeredV4).unsafeRunSync()

    result shouldBe Left(ErrorDetails("Failed to schedule comm", OrchestrationError))
  }

  it should "return successful result if a cancellationRequest is successful" in {
    val cancellationRequested = generate[CancellationRequestedV3]
    val schedules = Seq(
      Right(generate[Schedule].copy(triggeredV4 = Some(TestUtil.customerTriggeredV4))),
      Right(generate[Schedule].copy(triggeredV4 = Some(TestUtil.customerTriggeredV4)))
    )
    val removeFromPersistence = (customerId: CustomerId, commName: TemplateId) => schedules
    val removeSchedule        = (scheduledId: ScheduleId) => true
    Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested) shouldBe
      schedules.map(_.right.map(_.triggeredV4.get.metadata))
  }

  it should "capture an appropriate error if removing the schedule from pesistent storage fails" in {
    val cancellationRequested = generate[CancellationRequestedV3]
    val expectedError         = List(Left(ErrorDetails("Failed to remove from persistence", OrchestrationError)))

    val removeFromPersistence = (customerId: CustomerId, commName: TemplateId) => expectedError
    val removeSchedule        = (scheduledId: ScheduleId) => true
    Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested) shouldBe expectedError
  }

  it should "capture an appropriate error if removing the schedule from memory fails for a single record" in {
    val cancellationRequested = generate[CancellationRequestedV3]
    val successfulSchedule    = generate[Schedule].copy(triggeredV4 = Some(TestUtil.customerTriggeredV4))
    val failedSchedule        = generate[Schedule].copy(triggeredV4 = Some(TestUtil.customerTriggeredV4))
    val schedules             = Seq(Right(successfulSchedule), Right(failedSchedule))

    val removeFromPersistence = (customerId: CustomerId, commName: TemplateId) => schedules
    val removeSchedule = (scheduledId: ScheduleId) => {
      if (scheduledId == failedSchedule.scheduleId) false
      else true
    }

    val expectedError =
      Left(ErrorDetails(s"Failed to remove ${failedSchedule.scheduleId} schedule(s) from memory", OrchestrationError))
    val result = Scheduler.descheduleComm(removeFromPersistence, removeSchedule)(cancellationRequested)
    result should contain theSameElementsAs List(Right(successfulSchedule.triggeredV4.get.metadata), expectedError)
  }
}
