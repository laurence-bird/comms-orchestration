package com.ovoenergy.orchestration.scheduling

object Persistence {

  sealed trait SetAsOrchestratingResult
  case class Successful(schedule: Schedule) extends SetAsOrchestratingResult
  case object AlreadyBeingOrchestrated      extends SetAsOrchestratingResult
  case object Failed                        extends SetAsOrchestratingResult

  trait Orchestration {
    def attemptSetScheduleAsOrchestrating(scheduleId: ScheduleId): SetAsOrchestratingResult
    def setScheduleAsFailed(scheduleId: ScheduleId, reason: String): Unit
    def setScheduleAsComplete(scheduleId: ScheduleId): Unit
  }

  trait Listing {
    def listPendingSchedules(): List[Schedule]
    def listExpiredSchedules(): List[Schedule]
  }
}