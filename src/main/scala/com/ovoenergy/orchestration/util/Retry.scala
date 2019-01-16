package com.ovoenergy.orchestration.util

import cats.effect.{Async, Timer}

import scala.concurrent.duration._
import scala.util.control.NonFatal

trait Retry[F[_]] {
  def apply[A](fa: F[A], isRetriable: Throwable => Boolean = NonFatal.apply): F[A]
}

object Retry {

  type Strategy = FiniteDuration => FiniteDuration

  private val DefaultMaxRetries    = 10
  private val DefaultInitialDelay  = 250.milliseconds
  private val DefaultBackOffFactor = 2.0
  private val DefaultStrategy      = backOffStrategy(DefaultBackOffFactor)

  private def backOffStrategy(factor: Double): Strategy = { fd =>
    fd * factor match {
      case x: FiniteDuration => x
      case _                 => fd
    }
  }

  def backOff[F[_]: Timer: Async](maxRetries: Int = DefaultMaxRetries,
                                  initialDelay: FiniteDuration = DefaultInitialDelay,
                                  backOffFactor: Double = DefaultBackOffFactor): Retry[F] =
    instance[F](initialDelay, maxRetries, backOffStrategy(backOffFactor))

  def fixed[F[_]: Timer: Async](maxRetries: Int = DefaultMaxRetries,
                                fixedDelay: FiniteDuration = DefaultInitialDelay): Retry[F] =
    instance[F](fixedDelay, maxRetries, identity)

  def instance[F[_]: Timer: Async](delay: FiniteDuration = DefaultInitialDelay,
                                   maxAttempts: Int = DefaultMaxRetries,
                                   strategy: Strategy = DefaultStrategy) = new Retry[F] {
    override def apply[A](fa: F[A], isRetriable: Throwable => Boolean = NonFatal.apply): F[A] =
      fs2.Stream
        .retry(fa, delay, strategy, maxAttempts + 1, isRetriable)
        .compile
        .lastOrError
  }

}
