package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.Clock

import cats.effect.Async
import com.gu.scanamo.ScanamoAsync
import com.gu.scanamo.error.DynamoReadError
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.Context
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.gu.scanamo._
import com.gu.scanamo.error.{ConditionNotMet, DynamoReadError, TypeCoercionError}
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class AsyncPersistence(orchestrationExpiryMinutes: Int, context: Context, clock: Clock = Clock.systemUTC())(
    implicit ec: ExecutionContext)
    extends LoggingWithMDC
    with DynamoFormats {

  def storeSchedule[F[_]: Async]: Schedule => F[Option[Schedule]] = { commSchedule: Schedule =>
    log.debug(s"persisting schedule: ${commSchedule}")
    val ops    = context.table.put(commSchedule)
    val future = ScanamoAsync.exec(context.asyncDb)(ops)

    liftAsync(future).flatMap {
      case Some(Right(x)) => {
        log.debug(s"Persisted comm schedule: $commSchedule")
        Async[F].pure(Some(x))
      }
      case Some(Left(e)) =>
        Async[F].raiseError(new RuntimeException(s"Error reading DynamoDb response: ${e.toString}"))
      case None =>
        Async[F].pure(None)
    }
  }

  def retrieveSchedule[F[_]: Async](scheduleId: ScheduleId): F[Option[Schedule]] = {
    val future: Future[Option[Either[DynamoReadError, Schedule]]] =
      ScanamoAsync.get[Schedule](context.asyncDb)(context.table.name)('scheduleId -> scheduleId)

    liftAsync(future).flatMap {
      case Some(Left(error)) =>
        log.warn(s"Problem retrieving schedule: $scheduleId", error)
        Async[F].raiseError(new RuntimeException(s"Problem retrieving schedule: $scheduleId"))
      case Some(Right(schedule: Schedule)) => Async[F].pure(Some(schedule))
      case None                            => Async[F].pure(None)
    }
  }

  def liftAsync[Result, F[_]: Async](in: Future[Result]): F[Result] = {
    Async[F].async { cb =>
      in.onComplete {
        case Success(res) => cb(Right(res))
        case Failure(e)   => cb(Left(e))
      }
    }
  }

}