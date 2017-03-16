package com.ovoenergy.orchestration.retry

import java.io.IOException

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class RetrySpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  import com.ovoenergy.orchestration.retry.Retry._

  val exception = new IOException("Oh noes!")

  val onFailure = (_: Throwable) => ()

  behavior of "#retry"

  it should "succeed if the operation succeeds on the first attempt" in {
    val result = retry(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed(0))
    result should be(Right(Retry.Succeeded("yay", 1)))
  }

  it should "succeed if the operation fails on the first attempt but succeeds on the second" in {
    val result = retry(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed(1))
    result should be(Right(Retry.Succeeded("yay", 2)))
  }

  it should "succeed if the operation succeeds just before we give up" in {
    val result = retry(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed(4))
    result should be(Right(Retry.Succeeded("yay", 5)))
  }

  it should "fail if the operation fails on every attempt" in {
    val result = retry(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed(5))
    result should be(Left(Retry.Failed(5, exception)))
  }

  it should "work with attempts == 1" in {
    val config = RetryConfig(1, Backoff.retryImmediately)

    val success = retry(config, onFailure)(failNtimesThenSucceed(0))
    success should be(Right(Retry.Succeeded("yay", 1)))

    val failure = retry(config, onFailure)(failNtimesThenSucceed(1))
    failure should be(Left(Retry.Failed(1, exception)))
  }

  def failNtimesThenSucceed(n: Int): () => Try[String] = {
    var counter = 0
    () =>
      {
        if (counter < n) {
          counter = counter + 1
          Failure(exception)
        } else
          Success("yay")
      }
  }

  behavior of "#retryAsync"

  val actorSystem        = ActorSystem("test", ConfigFactory.empty())
  implicit val scheduler = actorSystem.scheduler

  override def afterAll(): Unit = {
    actorSystem.terminate()
  }

  it should "succeed if the operation succeeds on the first attempt" in {
    val result = retryAsync(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed_async(0))
    result.futureValue should be("yay")
  }

  it should "succeed if the operation fails on the first attempt but succeeds on the second" in {
    val result = retryAsync(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed_async(1))
    result.futureValue should be("yay")
  }

  it should "succeed if the operation succeeds just before we give up" in {
    val result = retryAsync(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed_async(4))
    result.futureValue should be("yay")
  }

  it should "fail if the operation fails on every attempt" in {
    val result = retryAsync(RetryConfig(5, Backoff.retryImmediately), onFailure)(failNtimesThenSucceed_async(5))
    result.failed.futureValue should be(exception)
  }

  it should "work with attempts == 1" in {
    val config = RetryConfig(1, Backoff.retryImmediately)

    val success = retryAsync(config, onFailure)(failNtimesThenSucceed_async(0))
    success.futureValue should be("yay")

    val failure = retryAsync(config, onFailure)(failNtimesThenSucceed_async(1))
    failure.failed.futureValue should be(exception)
  }

  def failNtimesThenSucceed_async(n: Int): () => Future[String] = {
    var counter = 0
    () =>
      {
        if (counter < n) {
          counter = counter + 1
          Future.failed(exception)
        } else
          Future.successful("yay")
      }
  }

  behavior of "exponential backoff"

  it should "double after each attempt" in {
    import scala.concurrent.duration._
    val backoff = Backoff.exponential(1.5 seconds, 2.0)
    backoff(1) should be(1.5 seconds)
    backoff(2) should be(3 seconds)
    backoff(3) should be(6 seconds)
    backoff(4) should be(12 seconds)
  }

}
