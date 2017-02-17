package com.ovoenergy.orchestration.kafka.producers

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.Cancelled
import com.ovoenergy.orchestration.kafka.Serialisation
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object CancelledProducer extends LoggingWithMDC {

  def apply(hosts: String, topic: String)(implicit actorSystem: ActorSystem,
                                          materializer: Materializer): Cancelled => Future[RecordMetadata] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.cancelledSerializer, hosts))

    (cancelled: Cancelled) =>
      {
        logDebug(cancelled.metadata.traceToken, s"Posting event to $topic - $cancelled")
        producer.send(new ProducerRecord[String, Cancelled](topic, cancelled.metadata.customerId, cancelled))
      }
  }

  override def loggerName: String = "CancelledProducer"
}
