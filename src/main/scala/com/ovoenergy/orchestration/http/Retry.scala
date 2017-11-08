package com.ovoenergy.orchestration.http

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

object Retry {

  case class RetryConfig(attempts: Int, initialInterval: FiniteDuration, exponent: Double) {
    val backoff: (Int) => FiniteDuration = {
      Retry.Backoff.exponential(initialInterval, exponent)
    }
  }

  case class Failed(attemptsMade: Int, finalException: Throwable)
      extends Exception(
        s"Operation failed after $attemptsMade attempts. The final exception message was: ${finalException.getMessage}",
        finalException)

  case class Succeeded[A](result: A, attempts: Int)

  /**
    * Attempt to perform an operation up to a given number of times, then give up.
    *
    * @param shouldStop A hook that is called after each failure. Useful for logging, and handling failure conditions which require no retry.
    * @param f         The operation to perform.
    */
  def retry[A](config: RetryConfig, shouldStop: Throwable => Boolean)(f: () => Try[A]): Either[Failed, Succeeded[A]] = {
    @tailrec
    def rec(attempt: Int): Either[Failed, Succeeded[A]] = {
      f() match {
        case Success(result) => Right(Succeeded(result, attempt))
        case Failure(e) =>
          if (attempt == config.attempts || shouldStop(e)) {
            Left(Failed(attempt, e))
          } else {
            Thread.sleep(config.backoff(attempt).toMillis)
            rec(attempt + 1)
          }
      }
    }

    if (config.attempts <= 0)
      Left(Failed(0, new Exception(s"attempts must be >= 1, was ${config.attempts}")))
    else
      rec(1)
  }

  object Backoff {

    val retryImmediately = (_: Int) => Duration.Zero

    def constantDelay(interval: FiniteDuration) = (_: Int) => interval

    def exponential(initialInterval: FiniteDuration, exponent: Double) = (attemptsSoFar: Int) => {
      Duration.fromNanos((initialInterval.toNanos * Math.pow(exponent, attemptsSoFar - 1)).toLong)
    }

  }
}
