package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model.{OrchestratedEmail, OrchestratedEmailV2}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object OrchestratedEmailProducer extends LoggingWithMDC {

  def apply(hosts: String, topic: String)
           (implicit actorSystem: ActorSystem, materializer: Materializer): OrchestratedEmailV2 => Future[_] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.orchestratedEmailV2Serializer, hosts))

    (email: OrchestratedEmailV2) => {
      logInfo(email.metadata.traceToken, s"Posting event to $topic - $email")
      val future = producer.send(new ProducerRecord[String, OrchestratedEmailV2](topic, email.metadata.customerId, email))

      import scala.concurrent.ExecutionContext.Implicits.global
      future.foreach {
        r => logInfo(email.metadata.traceToken, s"Posted event to $topic partion ${r.partition()} offset ${r.offset()}")
      }
      future
    }
  }

  override def loggerName: String = "OrchestratedEmailProducer"
}
