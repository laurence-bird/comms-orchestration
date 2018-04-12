package com.ovoenergy.orchestration.kafka

import java.nio.file.Paths
import java.util.UUID

import cats.effect.Async

import scala.util.control.NonFatal
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.serialisation.Retry
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

object Producer {

  def apply[E: SchemaFor: ToRecord](topic: Topic[E]): Either[Retry.Failed, KafkaProducer[String, E]] = {

    val initialSettings = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG  -> topic.kafkaConfig.hosts,
      ProducerConfig.CLIENT_ID_CONFIG          -> s"comms-http-api-${topic.name}-${UUID.randomUUID()}",
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true",
      ProducerConfig.RETRIES_CONFIG            -> "5",
      ProducerConfig.ACKS_CONFIG               -> "all"
    )

    val sslSettings = topic.kafkaConfig.ssl.map { ssl =>
      Map(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG      -> Paths.get(ssl.keystore.location).toAbsolutePath.toString,
        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG          -> "PKCS12",
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG      -> ssl.keystore.password,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG           -> ssl.keyPassword,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG    -> Paths.get(ssl.truststore.location).toString,
        SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG        -> "JKS",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG    -> ssl.truststore.password
      ) ++ initialSettings
    }

    val producerSettings: Map[String, AnyRef] = sslSettings.getOrElse(initialSettings)

    topic.serializer
      .map { valueSerialiser =>
        new KafkaProducer[String, E](producerSettings.asJava, new StringSerializer(), valueSerialiser)
      }
  }

  def publisher[E, F[_]: Async](getKey: E => String, producer: KafkaProducer[String, E], topicName: String)(t: E) = {

    val record = new ProducerRecord[String, E](
      topicName,
      getKey(t),
      t
    )

    Async[F].async[RecordMetadata] { cb =>
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            Option(exception).fold(cb(Right(metadata)))(e => cb(Left(e)))
          }
        }
      )
    }
  }
}
