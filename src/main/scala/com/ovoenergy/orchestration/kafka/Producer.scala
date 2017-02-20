package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.orchestration.domain.HasIds
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.concurrent.Future

object Producer extends LoggingWithMDC {

  def apply[A](hosts: String, topic: String, serialiser: Serializer[A])(
      implicit actorSystem: ActorSystem,
      materializer: Materializer,
      hasids: HasIds[A]): A => Future[RecordMetadata] = {
    val producer = KafkaProducer(Conf(new StringSerializer, serialiser, hosts))

    (event: A) =>
      {
        logDebug(hasids.traceToken(event), s"Posting event to $topic - $topic")
        producer.send(new ProducerRecord[String, A](topic, hasids.customerId(event), event))
      }
  }
}
