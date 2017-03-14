package com.ovoenergy.orchestration.retry

import akka.actor.Scheduler
import akka.pattern.after

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

object Retry {

  case class Failed(attemptsMade: Int, finalException: Throwable)
      extends Exception(
        s"Operation failed after $attemptsMade attempts. The final exception message was: ${finalException.getMessage}",
        finalException)

  case class Succeeded[A](result: A, attempts: Int)

  /**
    * @param attempts The total number of attempts to make, including both the first attempt and any retries.
    * @param backoff Sleep between attempts. The number of attempts made so far is passed as an argument.
    */
  case class RetryConfig(attempts: Int, backoff: Int => FiniteDuration)

  /**
    * Attempt to perform an operation up to a given number of times, then give up.
    *
    * @param onFailure A hook that is called after each failure. Useful for logging.
    * @param f The operation to perform.
    */
  def retry[A](config: RetryConfig, onFailure: Throwable => Unit)(f: () => Try[A]): Either[Failed, Succeeded[A]] = {
    @tailrec
    def rec(attempt: Int): Either[Failed, Succeeded[A]] = {
      f() match {
        case Success(result) => Right(Succeeded(result, attempt))
        case Failure(e) =>
          onFailure(e)
          if (attempt == config.attempts) {
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

  /**
    * Attempt to perform an asynchronous operation up to a given number of times, then give up.
    *
    * Based on https://gist.github.com/viktorklang/9414163
    *
    * @param onFailure A hook that is called after each failure. Useful for logging.
    * @param f the operation to perform.
    */
  def retryAsync[A](config: RetryConfig, onFailure: Throwable => Unit, attempt: Int = 1)(f: () => Future[A])(implicit ec: ExecutionContext, s: Scheduler): Future[A] = {
    f() recoverWith {
      case e if attempt < config.attempts =>
        onFailure(e)
        val delay = config.backoff(attempt)
        after(delay, s)(retryAsync(config, onFailure, attempt + 1)(f))
    }
  }

  object Backoff {

    val retryImmediately = (_: Int) => Duration.Zero

    def constantDelay(interval: FiniteDuration) = (_: Int) => interval

    def exponential(initialInterval: FiniteDuration, exponent: Double) = (attemptsSoFar: Int) => {
      initialInterval * Math.pow(exponent, attemptsSoFar - 1)
    }

  }

}
