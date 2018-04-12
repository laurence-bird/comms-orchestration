package com.ovoenergy.orchestration.scheduling

import java.time.Instant
import java.util.{Date, Properties}

import org.quartz.JobBuilder._
import org.quartz.TriggerBuilder._
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object QuartzScheduling {

  private val quartzProperties = {
    val properties = new Properties()
    properties.put("org.quartz.scheduler.instanceName", "CommsScheduler")
    properties.put("org.quartz.threadPool.threadCount", "20")
    properties.put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore")
    properties
  }

  private val quartzScheduler = {
    val sf = new StdSchedulerFactory()
    sf.initialize(quartzProperties)
    sf.getScheduler
  }

  private val log = LoggerFactory.getLogger("JobScheduler")

  class OrchestrationJob extends Job {
    private def getAs[T](key: ScheduleId)(implicit jobDataMap: JobDataMap): T = jobDataMap.get(key).asInstanceOf[T]

    override def execute(context: JobExecutionContext): Unit = {
      implicit val jobDataMap = context.getJobDetail.getJobDataMap

      val scheduleId = getAs[ScheduleId]("scheduleId")
      val function   = getAs[ScheduleId => _]("function")

      log.info(s"Executing scheduled orchestration, scheduleId: $scheduleId")
      function.apply(scheduleId)
    }
  }

  def init(): Unit = {
    quartzScheduler.start()
  }

  def shutdown(): Unit = quartzScheduler.shutdown()

  def addSchedule(orchestrationFunction: (ScheduleId) => _)(scheduleId: ScheduleId, startAt: Instant): Boolean = {
    val jobKey = new JobKey(scheduleId)
    if (quartzScheduler.getJobDetail(jobKey) == null) {
      val jobDetail = newJob(classOf[OrchestrationJob])
        .withIdentity(jobKey)
        .usingJobData(buildJobDataMap(scheduleId, orchestrationFunction))
        .build()
      val trigger = newTrigger()
        .withIdentity(new TriggerKey(scheduleId.toString))
        .startAt(Date.from(startAt))
        .build()

      Try(quartzScheduler.scheduleJob(jobDetail, trigger)) match {
        case Success(_) =>
          true
        case Failure(e) =>
          log.warn(s"Failed to schedule comm (scheduleId: $scheduleId)", e)
          false
      }
    } else {
      // job is already scheduled, nothing to do
      false
    }
  }

  def removeSchedule(scheduleId: ScheduleId): Boolean = {
    val jobKey = new JobKey(scheduleId)
    Try(quartzScheduler.deleteJob(jobKey)) match {
      case Success(_) =>
        log.debug(s"Descheduled comm for (scheduleId: $scheduleId)")
        true
      case Failure(e) =>
        log.warn(s"Failed to remove scheduled comm for scheduleID: $scheduleId")
        false
    }
  }

  private def buildJobDataMap(scheduleId: ScheduleId, orchestrationFunction: (ScheduleId) => _): JobDataMap = {
    val map = new JobDataMap()
    map.put("scheduleId", scheduleId)
    map.put("function", orchestrationFunction)
    map
  }

}
