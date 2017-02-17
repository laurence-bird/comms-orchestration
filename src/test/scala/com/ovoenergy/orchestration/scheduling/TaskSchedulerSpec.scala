package com.ovoenergy.orchestration.scheduling

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class TaskSchedulerSpec extends FlatSpec with BeforeAndAfterAll with Eventually with Matchers {

  val functionInnvocations = mutable.MutableList[String]()
  def orchestrationFunction = (scheduleId: String) => {
    functionInnvocations += scheduleId
    Right(Future(()))
  }

  val addSchedule = QuartzScheduling.addSchedule(orchestrationFunction) _

  override def beforeAll() = {
    QuartzScheduling.init()
  }

  override def afterAll() = {
    QuartzScheduling.shutdown()
  }

  behavior of "JobScheduler"

  it should "execute job when scheduled immediately" in {
    val scheduleId = "1231231331"
    addSchedule(scheduleId, Instant.now()) shouldBe true
    eventually {
      functionInnvocations should contain(scheduleId)
    }(PatienceConfig(scaled(Span(1, Seconds)), scaled(Span(5, Millis))))
  }

  it should "execute job when scheduled for future" in {
    val scheduleId = "4324234242"
    addSchedule(scheduleId, Instant.now().plusSeconds(2)) shouldBe true
    Thread.sleep(1000)
    functionInnvocations should not contain scheduleId
    Thread.sleep(1000)
    eventually {
      functionInnvocations should contain(scheduleId)
    }(PatienceConfig(scaled(Span(1, Seconds)), scaled(Span(5, Millis))))
  }

  it should "not schedule a job if it has already been scheduled" in {
    val scheduleId = "123"
    addSchedule(scheduleId, Instant.now().plusSeconds(600)) shouldBe true
    addSchedule(scheduleId, Instant.now()) shouldBe false
  }

}
