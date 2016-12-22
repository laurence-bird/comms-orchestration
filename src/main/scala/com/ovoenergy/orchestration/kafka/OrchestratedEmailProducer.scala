package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.OrchestratedEmail
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object OrchestratedEmailProducer extends LoggingWithMDC {

  def apply(hosts: String, topic: String)
           (implicit actorSystem: ActorSystem, materializer: Materializer): OrchestratedEmail => Future[_] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.orchestratedEmailSerializer, hosts))

    (email: OrchestratedEmail) => {
      logInfo(email.metadata.traceToken, s"Posting event to $topic - $email")
      val future = producer.send(new ProducerRecord[String, OrchestratedEmail](topic, email.metadata.customerId, email))

      import scala.concurrent.ExecutionContext.Implicits.global
      future.foreach {
        r => logInfo(email.metadata.traceToken, s"Posted event to $topic partion ${r.partition()} offset ${r.offset()}")
      }
      future
    }
  }

  override def loggerName: String = "OrchestratedEmailProducer"
}
