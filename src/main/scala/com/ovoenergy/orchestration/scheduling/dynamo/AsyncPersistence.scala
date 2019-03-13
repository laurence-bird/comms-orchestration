package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.Clock

import cats.effect.Async
import com.gu.scanamo.ScanamoAsync
import com.gu.scanamo.error.{DynamoReadError, ScanamoError}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.Context
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.gu.scanamo._
import com.gu.scanamo.query.{AndCondition, Condition}
import com.gu.scanamo.syntax._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class AsyncPersistence(context: Context, clock: Clock = Clock.systemUTC())(implicit ec: ExecutionContext)
    extends LoggingWithMDC
    with DynamoFormats {

  def storeSchedule[F[_]: Async](commSchedule: Schedule): F[Option[Schedule]] = {

    val ops    = context.table.put(commSchedule)
    def future = ScanamoAsync.exec(context.db)(ops)

    Async[F].delay(debug(commSchedule)(s"persisting schedule")) >> liftAsync(future).flatMap {
      case Some(Right(x)) =>
        Async[F].delay(debug(commSchedule)(s"Persisted comm schedule")).as(Some(x))
      case Some(Left(e)) =>
        Async[F].raiseError(new RuntimeException(s"Error reading DynamoDb response: ${e.toString}"))
      case None =>
        Async[F].pure(None)
    }
  }

  def retrieveSchedule[F[_]: Async](scheduleId: ScheduleId): F[Option[Schedule]] = {
    def future: Future[Option[Either[DynamoReadError, Schedule]]] =
      ScanamoAsync.get[Schedule](context.db)(context.table.name)('scheduleId -> scheduleId)

    liftAsync(future).flatMap {
      case Some(Left(error: DynamoReadError)) =>
        Async[F].delay(warn(s"Problem retrieving schedule: $scheduleId, ${DynamoReadError.describe(error)}")) >> Async[
          F].raiseError(new RuntimeException(s"Problem retrieving schedule: $scheduleId"))
      case Some(Right(schedule: Schedule)) => Async[F].pure(Some(schedule))
      case None                            => Async[F].pure(None)
    }
  }

  def liftAsync[Result, F[_]: Async](in: => Future[Result]): F[Result] = {
    Async[F].async { cb =>
      in.onComplete {
        case Success(res) => cb(Right(res))
        case Failure(e)   => cb(Left(e))
      }
    }
  }

}
