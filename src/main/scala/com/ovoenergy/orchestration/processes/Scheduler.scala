package com.ovoenergy.orchestration.processes

import java.time.{Clock, Instant}

import cats.effect.Async
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId}
import cats.syntax.either._
import scala.util.control.NonFatal
import scala.util.Try
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.either._
import cats.syntax.applicative._
import cats.syntax.applicativeError._

object Scheduler extends LoggingWithMDC {
  type CustomerId = String
  type CommName   = String

  def scheduleComm[F[_]: Async](storeInDb: Schedule => F[Option[Schedule]],
                                registerTask: (ScheduleId, Instant) => Boolean,
                                clock: Clock = Clock.systemUTC()): TriggeredV3 => F[Either[ErrorDetails, Boolean]] = {
    (triggered: TriggeredV3) =>
      val schedule: Schedule = Schedule.buildFromTrigger(triggered, clock)
      val scheduleInstant    = schedule.deliverAt
      info(schedule)("Storing comm in schedule")

      storeInDb(schedule)
        .flatMap { s =>
          Async[F]
            .delay {
              registerTask(schedule.scheduleId, scheduleInstant)
            }
            .map(task => task.asRight[ErrorDetails])
        }
        .recover {
          case NonFatal(e) =>
            failWithException(schedule)("Failed to schedule comm")(e)
            Left(ErrorDetails("Failed to schedule comm", OrchestrationError))
        }
  }

  // TODO: make me Async!
  def descheduleComm(removeFromDb: (CustomerId, CommName) => Seq[Either[ErrorDetails, Schedule]],
                     removeTask: (ScheduleId) => Boolean)(
      cancellationRequested: CancellationRequestedV2): Seq[Either[ErrorDetails, MetadataV2]] = {

    def removeScheduleFromMemory(schedule: Schedule): Either[ErrorDetails, MetadataV2] = {
      // Filter out failed schedule removals
      if (removeTask(schedule.scheduleId)) {
        schedule.triggeredV3 match {
          case Some(triggered) => Right(triggered.metadata)
          case None =>
            Left(
              ErrorDetails(s"Unable to orchestrate as no Triggered event in Schedule: $schedule", OrchestrationError))
        }
      } else {
        Left(ErrorDetails(s"Failed to remove ${schedule.scheduleId} schedule(s) from memory", OrchestrationError))
      }
    }

    debug(cancellationRequested)(s"Processing request: $cancellationRequested")
    val dynamoResult = removeFromDb(cancellationRequested.customerId, cancellationRequested.commName)
    dynamoResult.map {
      case Left(err) => {
        fail(err)(s"Failed to remove schedule from db: ${err.reason}")
        Left(err)
      }
      case Right(schedule) => {
        debug(schedule)("Removing sschedule from memory")
        removeScheduleFromMemory(schedule)
      }
    }
  }
}
