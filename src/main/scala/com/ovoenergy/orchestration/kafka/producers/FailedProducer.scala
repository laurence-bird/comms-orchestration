package com.ovoenergy.orchestration.kafka.producers

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.kafka.Serialisation
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object FailedProducer extends LoggingWithMDC {

  def failedTrigger(hosts: String, topic: String)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer): (String, TriggeredV2, ErrorCode, InternalMetadata) => Future[RecordMetadata] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.failedSerializer, hosts))

    (reason: String, triggeredV2: TriggeredV2, errorCode: ErrorCode, internalMetadata: InternalMetadata) =>
      {
        val failed = Failed(
          metadata = Metadata.fromSourceMetadata("orchestration", triggeredV2.metadata),
          reason = reason,
          errorCode = errorCode,
          internalMetadata = internalMetadata
        )

        logWarn(failed.metadata.traceToken, s"Posting event to $topic - $failed")
        producer.send(new ProducerRecord[String, Failed](topic, failed.metadata.customerId, failed))
      }
  }

  def failedCancellation(hosts: String, topic: String)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer): FailedCancellation => Future[RecordMetadata] = {
    val producer = KafkaProducer(Conf(new StringSerializer, Serialisation.failedCancellationSerializer, hosts))

    (failedCancellationRequest: FailedCancellation) =>
      {
        logWarn(failedCancellationRequest.cancellationRequested.metadata.traceToken,
                s"Posting event to $topic - $failedCancellationRequest")
        producer.send(
          new ProducerRecord[String, FailedCancellation](topic,
                                                         failedCancellationRequest.cancellationRequested.customerId,
                                                         failedCancellationRequest))
      }
  }

  override def loggerName: String = "FailedProducer"
}
