package com.ovoenergy.orchestration.util

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RetrySpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 5.seconds)
  implicit val ec: ExecutionContext                    = ExecutionContext.global
  implicit val timer                                   = cats.effect.IO.timer(ec)

  "retry" when {

    "task succeeded" should {
      "return the result" in {
        Retry.instance[IO]().apply(IO(1)).attempt.unsafeToFuture().futureValue shouldBe Right(1)
      }
    }

    "task consistently fail" should {
      "fail" in {
        val expectedFailure = new RuntimeException
        val r = Retry
          .instance[IO](maxAttempts = 3, delay = 10.milliseconds)
          .apply(IO.raiseError(expectedFailure))
          .attempt
          .unsafeToFuture()
          .futureValue
        r shouldBe Left(expectedFailure)
      }
    }

    "task succeeded after 3 attempts" when {
      "maxRetries is >= 3" should {
        "succeeded" in {
          val counter        = new AtomicInteger(1)
          val expectedResult = 1

          def task = IO {
            val c = counter.getAndIncrement()
            if (c > 3) {
              expectedResult
            } else throw new RuntimeException
          }

          val retry = Retry.instance[IO](maxAttempts = 3, delay = 10.milliseconds)
          retry(task).attempt.unsafeToFuture().futureValue shouldBe Right(expectedResult)
        }
      }

      "maxRetries is < 3" should {
        "fail" in {
          val counter         = new AtomicInteger(1)
          val expectedFailure = new RuntimeException

          def task = IO(if (counter.getAndIncrement() > 3) 1 else throw expectedFailure)

          val retry = Retry.instance[IO](maxAttempts = 2, delay = 10.milliseconds)
          retry(task).attempt.unsafeToFuture().futureValue shouldBe Left(expectedFailure)
        }
      }
    }

    "task succeeded after 150 milliseconds" when {
      "maxDelay is > 150 milliseconds" should {
        "succeeded" in {
          val start          = System.currentTimeMillis()
          val expectedResult = 1

          def task = IO(if (System.currentTimeMillis() - start > 150) expectedResult else throw new RuntimeException)

          val retry = Retry.instance[IO](maxAttempts = 4, delay = 50.milliseconds)
          retry(task).attempt.unsafeToFuture().futureValue shouldBe Right(expectedResult)
        }
      }

      "maxDelay is < 150 milliseconds" should {
        "fail" in {
          val start           = System.currentTimeMillis()
          val expectedFailure = new RuntimeException

          def task = IO(if (System.currentTimeMillis() - start > 150) 1 else throw expectedFailure)

          val retry = Retry.fixed[IO](maxRetries = 3, fixedDelay = 25.milliseconds)
          retry(task).attempt.unsafeToFuture().futureValue shouldBe Left(expectedFailure)
        }
      }
    }
  }
}
