package com.ovoenergy.orchestration.scheduling

import com.ovoenergy.comms.model.TriggeredV2
import org.quartz.impl.StdSchedulerFactory

import scala.concurrent.Future

object ScheduleManager {

  private val quartzScheduler = StdSchedulerFactory.getDefaultScheduler

  def init(orchestrator: TriggeredV2 => Future[_], onFailure: (String, TriggeredV2) => Future[_]): Unit = {
    quartzScheduler.start()

  }

  def scheduleComm(): Unit = {

  }

}


