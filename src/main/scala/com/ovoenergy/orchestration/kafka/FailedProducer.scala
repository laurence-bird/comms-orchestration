package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.Failed
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object FailedProducer extends LoggingWithMDC {

  def apply(hosts: String, topic: String)
           (implicit actorSystem: ActorSystem, materializer: Materializer): Failed => Future[_] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.failedSerializer, hosts))

    (failed: Failed) => {
      logWarn(failed.metadata.traceToken, s"Posting event to $topic - $failed")
      producer.send(new ProducerRecord[String, Failed](topic, failed.metadata.customerId, failed))
    }
  }

  override def loggerName: String = "FailedProducer"
}
