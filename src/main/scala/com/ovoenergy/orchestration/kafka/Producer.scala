package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.orchestration.domain.HasIds
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.retry.Retry._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.concurrent.Future

object Producer extends LoggingWithMDC {

  def apply[A](hosts: String, topic: String, serialiser: Serializer[A], retryConfig: RetryConfig)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer,
      hasids: HasIds[A]): A => Future[RecordMetadata] = {

    implicit val scheduler = actorSystem.scheduler

    val producer = KafkaProducer(Conf(new StringSerializer, serialiser, hosts))

    (event: A) =>
      {
        logDebug(hasids.traceToken(event), s"Posting event to $topic")
        logInfo(hasids.traceToken(event), s"Posting event to $topic")

        import scala.concurrent.ExecutionContext.Implicits.global
        retryAsync(
          config = retryConfig,
          onFailure = e => logWarn(hasids.traceToken(event), s"Failed to send Kafka event to topic $topic", e)
        ) { () =>
          producer.send(new ProducerRecord[String, A](topic, hasids.customerId(event), event))
        }
      }
  }
}
