package com.ovoenergy.orchestration.kafka.producers

import java.nio.file.Paths
import java.util.UUID

import cats.effect.{IO, Timer}
import cats.syntax.flatMap._
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object Producer extends LoggingWithMDC {

  def apply[E: SchemaFor: ToRecord](topic: Topic[E]): KafkaProducer[String, E] = {

    val initialSettings = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> topic.kafkaConfig.hosts,
      ProducerConfig.CLIENT_ID_CONFIG         -> s"comms-orchestrator-${topic.name}-${UUID.randomUUID()}",
      // TODO Enable idempotency back when https://issues.apache.org/jira/browse/KAFKA-6817 is solved
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> "false",
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1",
      ProducerConfig.RETRIES_CONFIG                        -> "5",
      ProducerConfig.ACKS_CONFIG                           -> "all"
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

    new KafkaProducer[String, E](producerSettings.asJava, new StringSerializer(), topic.serializer)
  }

  def publisher[E](getKey: E => String, producer: KafkaProducer[String, E], topicName: String)(t: E)(
      implicit ec: ExecutionContext = ExecutionContext.global) = {

    val record = new ProducerRecord[String, E](
      topicName,
      getKey(t),
      t
    )

    val produce = IO.async[RecordMetadata] { cb =>
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            Option(exception).fold(cb(Right(metadata)))(e => cb(Left(e)))
          }
        }
      )
    }

    for {
      rm <- produce
      _  <- IO.shift(Timer[IO])
    } yield rm
  }

  def publisherFor[E: Loggable](topic: Topic[E], key: E => String)(implicit schemaFor: SchemaFor[E],
                                                                   toRecord: ToRecord[E],
                                                                   classTag: ClassTag[E]): E => IO[RecordMetadata] = {
    val producer: KafkaProducer[String, E] = Producer[E](topic)
    val publisher = { e: E =>
      Producer
        .publisher[E](key, producer, topic.name)(e)
        .flatTap((rm: RecordMetadata) => IO(info((rm, e))(s"Sent event to ${topic.name}")))
    }

    publisher
  }
}
