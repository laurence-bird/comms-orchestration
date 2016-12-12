package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.ComposedEmail
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

class ComposedEmailProducer extends LoggingWithMDC {

  def build(hosts: String, topic: String)
           (implicit actorSystem: ActorSystem, materializer: Materializer): ComposedEmail => Future[_] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.composedEmailSerializer, hosts))

    (progressed: ComposedEmail) => {
      logInfo(progressed.metadata.traceToken, s"Posting event to $topic - $progressed")
      producer.send(new ProducerRecord[String, ComposedEmail](topic, progressed))
    }
  }

  override def loggerName: String = "ComposedEmailProducer"
}
