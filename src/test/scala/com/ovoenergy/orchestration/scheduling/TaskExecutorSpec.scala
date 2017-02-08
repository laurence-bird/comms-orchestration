package com.ovoenergy.orchestration.scheduling

import com.ovoenergy.comms.model.{ErrorCode, InternalMetadata, TriggeredV2}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Failed, SetAsOrchestratingResult, Successful}
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskExecutorSpec extends FlatSpec with Matchers with OneInstancePerTest with ArbGenerator {

  trait StubPersistence extends Persistence.Orchestration {
    override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
      if (scheduleId == scheduleId) {
        Successful(schedule)
      } else fail("Incorrect scheduleId requested")
    }
    override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = fail("Incorrectly invoked")
    override def setScheduleAsComplete(scheduleId: ScheduleId): Unit = fail("Incorrectly invoked")
  }
  val scheduleId = "1234567890A"
  val schedule = generate[Schedule].copy(scheduleId = scheduleId)

  var triggerOrchestrated = Option.empty[(TriggeredV2, InternalMetadata)]
  val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
    triggerOrchestrated = Some(triggeredV2, internalMetadata)
    Right(Future(()))
  }

  val traceToken = "ssfifjsof"
  val generateTraceToken = () => traceToken

  var failedEventSent = Option.empty[(String, TriggeredV2, ErrorCode, InternalMetadata)]
  val sendFailedEvent = (reason: String, triggeredV2: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata) => {
    failedEventSent = Some(reason, triggeredV2, errorCode, internalMetadata)
    Future(())
  }

  behavior of "TaskExecutor"

  it should "handle comm already being orchestrated" in {
    object AlreadyOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) AlreadyBeingOrchestrated
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(AlreadyOrchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
  }

  it should "handle failure setting comm schedule as orchestrating" in {
    object FailureOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) Failed
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(FailureOrchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
  }

  it should "handle orchestration failure" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) Successful(schedule)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(scheduleId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV2, internalMetadata)
      Left(ErrorDetails("Some error", ErrorCode.OrchestrationError))
    }

    TaskExecutor.execute(Orchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe Some("Some error", schedule.triggered, ErrorCode.OrchestrationError, InternalMetadata(traceToken))
    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }

  it should "handle orchestration timeout" in {

    var scheduleAsFailed = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
        scheduleAsFailed = Some(scheduleId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV2, internalMetadata)
      val future = Future { Thread.sleep(11000) }
      Right(future)
    }

    TaskExecutor.execute(Orchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe Some("Orchestrating comm timed out", schedule.triggered, ErrorCode.OrchestrationError, InternalMetadata(traceToken))
    scheduleAsFailed shouldBe Some(scheduleId, "Orchestrating comm timed out")
  }

  it should "should orchestrate" in {

    var scheduleAsComplete = Option.empty[ScheduleId]
    object Orchestrating extends StubPersistence {
      override def setScheduleAsComplete(scheduleId: ScheduleId): Unit = {
        scheduleAsComplete = Some(scheduleId)
      }
    }
    val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV2, internalMetadata)
      Right(Future())
    }

    TaskExecutor.execute(Orchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe None
    scheduleAsComplete shouldBe Some(scheduleId)
  }

  it should "handle send failed event timeout" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) Successful(schedule)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(scheduleId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV2, internalMetadata)
      Left(ErrorDetails("Some error", ErrorCode.OrchestrationError))
    }
    val sendFailedEvent = (reason: String, triggeredV2: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata) => {
      failedEventSent = Some(reason, triggeredV2, errorCode, internalMetadata)
      Future { Thread.sleep(6000) }
    }

    TaskExecutor.execute(Orchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe Some("Some error", schedule.triggered, ErrorCode.OrchestrationError, InternalMetadata(traceToken))
    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }

  it should "handle send failed event failure" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) Successful(schedule)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(scheduleId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV2, internalMetadata)
      Left(ErrorDetails("Some error", ErrorCode.OrchestrationError))
    }
    val sendFailedEvent = (reason: String, triggeredV2: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata) => {
      failedEventSent = Some(reason, triggeredV2, errorCode, internalMetadata)
      Future {throw new RuntimeException("failing the future") }
    }

    TaskExecutor.execute(Orchestrating, orchestrateTrigger, generateTraceToken, sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe Some("Some error", schedule.triggered, ErrorCode.OrchestrationError, InternalMetadata(traceToken))
    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }



}
