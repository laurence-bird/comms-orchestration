package com.ovoenergy.orchestration.scheduling

import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  SetAsOrchestratingResult,
  Successful,
  Failed => FailedPersistence
}
import com.ovoenergy.orchestration.util.ArbGenerator
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Record
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskExecutorSpec extends FlatSpec with Matchers with OneInstancePerTest with ArbGenerator with Eventually {

  trait StubPersistence extends Persistence.Orchestration {
    override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
      if (scheduleId == scheduleId) {
        Successful(schedule)
      } else fail("Incorrect scheduleId requested")
    }
    override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit = fail("Incorrectly invoked")
    override def setScheduleAsComplete(scheduleId: ScheduleId): Unit               = fail("Incorrectly invoked")
  }
  val scheduleId = "1234567890A"
  val schedule   = generate[Schedule].copy(scheduleId = scheduleId)

  val recordMetadata      = new RecordMetadata(new TopicPartition("test", 1), 1, 1, Record.NO_TIMESTAMP, -1, -1, -1)
  var triggerOrchestrated = Option.empty[(TriggeredV2, InternalMetadata)]
  val orchestrateTrigger = (triggeredV2: TriggeredV2, internalMetadata: InternalMetadata) => {
    triggerOrchestrated = Some(triggeredV2, internalMetadata)
    Right(Future.successful(recordMetadata))
  }

  val sendOrchestrationStartedEvent = (orchStarted: OrchestrationStarted) => Future.successful(recordMetadata)

  val traceToken         = "ssfifjsof"
  val generateTraceToken = () => traceToken

  var failedEventSent = Option.empty[Failed]
  val sendFailedEvent =
    (failed: Failed) => {
      failedEventSent = Some(failed)
      Future(recordMetadata)
    }

  behavior of "TaskExecutor"

  it should "handle comm already being orchestrated" in {
    object AlreadyOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) AlreadyBeingOrchestrated
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(AlreadyOrchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
  }

  it should "handle failure setting comm schedule as orchestrating" in {
    object FailureOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == scheduleId) FailedPersistence
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(FailureOrchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

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

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    failedEventSent shouldBe Some(
      Failed(schedule.triggered.metadata, InternalMetadata(traceToken), "Some error", ErrorCode.OrchestrationError))
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
      val future = Future[RecordMetadata] { Thread.sleep(11000); recordMetadata }
      Right(future)
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    eventually {
      failedEventSent shouldBe Some(
        Failed(schedule.triggered.metadata,
               InternalMetadata(traceToken),
               "Orchestrating comm timed out",
               ErrorCode.OrchestrationError))
      scheduleAsFailed shouldBe Some(scheduleId, "Orchestrating comm timed out")
    }(PatienceConfig(Span(11, Seconds)))
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
      Right(Future(recordMetadata))
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

    eventually {
      //side effects
      triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
      failedEventSent shouldBe None
      scheduleAsComplete shouldBe Some(scheduleId)
    }(PatienceConfig(Span(3, Seconds)))
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

    var sendFailedEventInvoked = false
    val timedOutSendFailedEvent: (Failed) => Future[RecordMetadata] =
      (failed: Failed) => {
        sendFailedEventInvoked = true
        Future { Thread.sleep(6000); recordMetadata }
      }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         timedOutSendFailedEvent)(scheduleId)

    //side effects
    eventually {
      triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
      sendFailedEventInvoked shouldBe true
      scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
    }(PatienceConfig(Span(6, Seconds)))
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
    var sendFailedEventInvoked = false
    val sendFailedEvent =
      (failed: Failed) => {
        sendFailedEventInvoked = true
        Future { throw new RuntimeException("failing the future"); null }
      }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(schedule.triggered, InternalMetadata(traceToken))
    sendFailedEventInvoked shouldBe true
    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }

}
