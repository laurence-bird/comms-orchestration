package com.ovoenergy.orchestration.scheduling

import java.time.Instant
import java.util.Date

import org.quartz.JobBuilder._
import org.quartz.TriggerBuilder._
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory

object TaskScheduler {

  private val quartzScheduler = StdSchedulerFactory.getDefaultScheduler
  private val log = LoggerFactory.getLogger("JobScheduler")

  class OrchestrationJob extends Job {
    private def getAs[T](key: ScheduleId)(implicit jobDataMap: JobDataMap): T = jobDataMap.get(key).asInstanceOf[T]

    override def execute(context: JobExecutionContext): Unit = {
      implicit val jobDataMap = context.getJobDetail.getJobDataMap

      val scheduleId = getAs[ScheduleId]("scheduleId")
      val function = getAs[ScheduleId => _]("function")

      log.info(s"Executing scheduled orchestration, scheduleId: $scheduleId")
      function.apply(scheduleId)
    }
  }

  def init(): Unit = {
    quartzScheduler.start()
  }

  def addSchedule(orchestrationFunction: (ScheduleId) => _)(scheduleId: ScheduleId, startAt: Instant): Unit = {
    val jobDetail = newJob(classOf[OrchestrationJob])
      .withIdentity(new JobKey(scheduleId))
      .usingJobData(buildJobDataMap(scheduleId, orchestrationFunction))
      .build()
    val trigger = newTrigger()
      .withIdentity(new TriggerKey(scheduleId.toString))
      .startAt(Date.from(startAt))
      .build()
    log.debug(s"Scheduled comm (scheduleId: $scheduleId) to orchestrate at ${Date.from(startAt)}")
    quartzScheduler.scheduleJob(jobDetail, trigger)
  }

  private def buildJobDataMap(scheduleId: ScheduleId, orchestrationFunction: (ScheduleId) => _): JobDataMap = {
    val map = new JobDataMap()
    map.put("scheduleId", scheduleId)
    map.put("function", orchestrationFunction)
    map
  }

}


