package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.stream.Materializer
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import com.ovoenergy.comms.akka.streams.Factory.SSLConfig
import com.ovoenergy.comms.model.LoggableEvent
import com.ovoenergy.comms.serialisation.{Serialisation => KafkaSerialisation}
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings
import com.ovoenergy.orchestration.domain.HasCommName
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.retry.Retry._
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.concurrent.Future

object Producer extends LoggingWithMDC {

  def apply[A <: LoggableEvent: ToRecord: SchemaFor](hosts: String,
                                                     topic: String,
                                                     schemaRegistryClientSettings: SchemaRegistryClientSettings,
                                                     sslConfig: Option[SSLConfig],
                                                     retryConfig: RetryConfig)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer,
      hasCommName: HasCommName[A]): A => Future[RecordMetadata] = {

    implicit val scheduler = actorSystem.scheduler
    val serialiser         = KafkaSerialisation.avroBinarySchemaRegistrySerializer[A](schemaRegistryClientSettings, topic)
    val producerConf = {
      val conf = Conf(new StringSerializer, serialiser, hosts)
      sslConfig.fold(conf) { ssl =>
        conf
          .withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
          .withProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ssl.keystoreLocation.toAbsolutePath.toString)
          .withProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ssl.keystoreType.toString)
          .withProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.keystorePassword)
          .withProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ssl.keyPassword)
          .withProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.truststoreLocation.toString)
          .withProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ssl.truststoreType.toString)
          .withProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.truststorePassword)
      }
    }
    val producer = KafkaProducer(producerConf)

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
