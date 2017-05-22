package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.LoggableEvent
import com.ovoenergy.orchestration.domain.HasCommName
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.retry.Retry._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.concurrent.Future

object Producer extends LoggingWithMDC {

  def apply[A <: LoggableEvent](hosts: String, topic: String, serialiser: Serializer[A], retryConfig: RetryConfig)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer,
      hasCommName: HasCommName[A]): A => Future[RecordMetadata] = {

    implicit val scheduler = actorSystem.scheduler

    val producer = KafkaProducer(Conf(new StringSerializer, serialiser, hosts))

    (event: A) =>
      {
        logDebug(event, s"Posting event to $topic")

        import scala.concurrent.ExecutionContext.Implicits.global
        retryAsync(
          config = retryConfig,
          onFailure = e => logWarn(event, s"Failed to send Kafka event to topic $topic", e)
        ) { () =>
          producer.send(new ProducerRecord[String, A](topic, hasCommName.commName(event), event)).map { record =>
            logInfo(event, s"Event posted to $topic: \n ${event.loggableString}")
            record
          }
        }
      }
  }
}
