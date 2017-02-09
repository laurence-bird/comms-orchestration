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

  val addSchedule = TaskScheduler.addSchedule(orchestrationFunction) _

  override def beforeAll() = {
    TaskScheduler.init()
  }

  behavior of "JobScheduler"

  it should "execute job when scheduled immediately" in {
    val scheduleId = "1231231331"
    addSchedule(scheduleId, Instant.now())
    eventually {
      functionInnvocations should contain(scheduleId)
    }(PatienceConfig(scaled(Span(1, Seconds)), scaled(Span(5, Millis))))
  }

  it should "execute job when scheduled for future" in {
    val scheduleId = "4324234242"
    addSchedule(scheduleId, Instant.now().plusSeconds(2))
    Thread.sleep(1000)
    functionInnvocations should not contain(scheduleId)
    Thread.sleep(1000)
    eventually {
      functionInnvocations should contain(scheduleId)
    }(PatienceConfig(scaled(Span(1, Seconds)), scaled(Span(5, Millis))))
  }



}
