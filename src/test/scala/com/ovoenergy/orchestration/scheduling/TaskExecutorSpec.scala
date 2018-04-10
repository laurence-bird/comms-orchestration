package com.ovoenergy.orchestration.scheduling

import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.ExecutionContexts
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.Persistence.{
  AlreadyBeingOrchestrated,
  SetAsOrchestratingResult,
  Successful,
  Failed => FailedPersistence
}
import com.ovoenergy.orchestration.util.{ArbGenerator, TestUtil}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Record
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}
import org.scalacheck.Shapeless._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

class TaskExecutorSpec
    extends FlatSpec
    with Matchers
    with OneInstancePerTest
    with ArbGenerator
    with Eventually
    with ExecutionContexts {

  trait StubPersistence extends Persistence.Orchestration {
    override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
      if (scheduleId == sId) {
        Successful(scheduleWithTriggeredV3)
      } else fail("Incorrect scheduleId requested")
    }
    override def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit =
      fail("Set schedule as failed incorrectly invoked")
    override def setScheduleAsComplete(scheduleId: ScheduleId): Unit =
      fail("Set schedule as complete Incorrectly invoked")
  }
  val scheduleId = "1234567890A"
  val scheduleWithTriggeredV3 =
    generate[Schedule].copy(scheduleId = scheduleId, triggeredV3 = Some(TestUtil.customerTriggered))

  val recordMetadata      = new RecordMetadata(new TopicPartition("test", 1), 1, 1, 100l, -1, -1, -1)
  var triggerOrchestrated = Option.empty[(TriggeredV3, InternalMetadata)]
  val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
    triggerOrchestrated = Some(triggeredV3, internalMetadata)
    Right(IO.pure(recordMetadata))
  }

  val sendOrchestrationStartedEvent = (orchStarted: OrchestrationStartedV2) => IO.pure(recordMetadata)

  val traceToken         = "ssfifjsof"
  val generateTraceToken = () => traceToken

  var failedEventSent = Option.empty[FailedV2]
  val sendFailedEvent =
    (failed: FailedV2) => {
      failedEventSent = Some(failed)
      IO.pure(recordMetadata)
    }

  behavior of "TaskExecutor"

  it should "handle comm already being orchestrated" in {
    object AlreadyOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) AlreadyBeingOrchestrated
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(AlreadyOrchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
  }

  it should "handle failure setting comm schedule as orchestrating" in {
    object FailureOrchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) FailedPersistence
        else fail("Incorrect scheduleId requested")
      }
    }

    TaskExecutor.execute(FailureOrchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
  }

  it should "handle orchestration failure" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) Successful(scheduleWithTriggeredV3)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(sId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(sId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV3, internalMetadata)
      Left(ErrorDetails("Some error", OrchestrationError))
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(scheduleWithTriggeredV3.triggeredV3.get, InternalMetadata(traceToken))
    failedEventSent shouldBe 'defined
    val failedEventResult = failedEventSent.get
    failedEventResult.internalMetadata shouldBe InternalMetadata(traceToken)
    failedEventResult.errorCode shouldBe OrchestrationError
    failedEventResult.reason shouldBe "Some error"

    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }

  it should "handle orchestration timeout" in {

    var scheduleAsFailed = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def setScheduleAsFailed(sId: ScheduleId, reason: String): Unit = {
        scheduleAsFailed = Some(sId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV3, internalMetadata)
      val future = Future[RecordMetadata] { Thread.sleep(22000); recordMetadata }
      Right(IO.fromFuture(IO(future)))
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(scheduleWithTriggeredV3.triggeredV3.get, InternalMetadata(traceToken))

    implicit val patienceConfig = PatienceConfig(Span(11, Seconds))
    eventually {
      failedEventSent shouldBe 'defined
      val failedEventResult = failedEventSent.get
      failedEventResult.internalMetadata shouldBe InternalMetadata(traceToken)
      failedEventResult.errorCode shouldBe OrchestrationError
      failedEventResult.reason shouldBe "Orchestrating comm timed out"
      scheduleAsFailed shouldBe Some(scheduleId, "Orchestrating comm timed out")
    }
  }

  it should "should orchestrate" in {

    var scheduleAsComplete = Option.empty[ScheduleId]
    object Orchestrating extends StubPersistence {
      override def setScheduleAsComplete(sId: ScheduleId): Unit = {
        scheduleAsComplete = Some(sId)
      }
    }
    val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV3, internalMetadata)
      Right(IO.pure(recordMetadata))
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    implicit val patienceConfig = PatienceConfig(Span(3, Seconds))
    eventually {
      //side effects
      triggerOrchestrated shouldBe Some(scheduleWithTriggeredV3.triggeredV3.get, InternalMetadata(traceToken))
      failedEventSent shouldBe None
      scheduleAsComplete shouldBe Some(scheduleId)
    }
  }

  it should "handle send failed event timeout" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) Successful(scheduleWithTriggeredV3)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(sId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(sId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV3, internalMetadata)
      Left(ErrorDetails("Some error", OrchestrationError))
    }

    var sendFailedEventInvoked = false
    val timedOutSendFailedEvent: (FailedV2) => IO[RecordMetadata] =
      (failed: FailedV2) => {
        sendFailedEventInvoked = true
        val f = Future { Thread.sleep(6000); recordMetadata }
        IO.fromFuture(IO(f))
      }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         timedOutSendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    implicit val patienceConfig = PatienceConfig(Span(6, Seconds))
    eventually {
      triggerOrchestrated shouldBe Some(scheduleWithTriggeredV3.triggeredV3.get, InternalMetadata(traceToken))
      sendFailedEventInvoked shouldBe true
      scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
    }
  }

  it should "handle send failed event failure" in {

    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) Successful(scheduleWithTriggeredV3)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(sId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(sId, reason)
      }
    }
    val orchestrateTrigger = (triggeredV3: TriggeredV3, internalMetadata: InternalMetadata) => {
      triggerOrchestrated = Some(triggeredV3, internalMetadata)
      Left(ErrorDetails("Some error", OrchestrationError))
    }
    var sendFailedEventInvoked = false
    val sendFailedEvent =
      (failed: FailedV2) => {
        sendFailedEventInvoked = true
        IO.raiseError(new RuntimeException("failing the future"))
      }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe Some(scheduleWithTriggeredV3.triggeredV3.get, InternalMetadata(traceToken))
    sendFailedEventInvoked shouldBe true
    scheduleFailedPersist shouldBe Some(scheduleId, "Some error")
  }

  it should "fail orchestration if schedule has no triggered events" in {
    val scheduleWithoutTriggered =
      generate[Schedule].copy(scheduleId = scheduleId, triggeredV3 = None)
    var scheduleFailedPersist = Option.empty[(ScheduleId, String)]
    object Orchestrating extends StubPersistence {
      override def attemptSetScheduleAsOrchestrating(sId: ScheduleId): SetAsOrchestratingResult = {
        if (scheduleId == sId) Successful(scheduleWithoutTriggered)
        else fail("Incorrect scheduleId requested")
      }
      override def setScheduleAsFailed(sId: ScheduleId, reason: String): Unit = {
        scheduleFailedPersist = Some(sId, reason)
      }
    }

    TaskExecutor.execute(Orchestrating,
                         orchestrateTrigger,
                         sendOrchestrationStartedEvent,
                         generateTraceToken,
                         sendFailedEvent,
                         globalExecutionContext)(scheduleId)

    //side effects
    triggerOrchestrated shouldBe None
    failedEventSent shouldBe None
    scheduleFailedPersist.get._1 shouldBe scheduleId
    scheduleFailedPersist.get._2 should include("Unable to orchestrate as no Triggered event in Schedule:")
  }

}
