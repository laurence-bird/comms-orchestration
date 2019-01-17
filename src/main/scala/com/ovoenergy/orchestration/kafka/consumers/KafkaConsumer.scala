package com.ovoenergy.orchestration.kafka.consumers

import java.nio.file.Paths

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.helpers.{KafkaClusterConfig, Topic}
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings
import com.ovoenergy.kafka.serialization.avro4s._
import com.typesafe.config.Config
import fs2.kafka._
import fs2._
import org.apache.kafka._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, reflectiveCalls}
import scala.concurrent.duration._
import com.sksamuel.avro4s.{FromRecord, SchemaFor}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.reflect.ClassTag

object KafkaConsumer {

  type Record[T] = ConsumerRecord[String, T]

  final case class Settings[T](deserialiser: Deserializer[Option[T]])

  val DefaultPollTimeout: FiniteDuration = 150.milliseconds

  def apply[F[_]] = new ApplyPartiallyApplied[F]

  class ApplyPartiallyApplied[F[_]] {

    def apply[T](topics: NonEmptyList[Topic[T]], kafkaConfig: KafkaClusterConfig)(
        pollTimeout: FiniteDuration = DefaultPollTimeout)(f: Record[T] => F[Unit])(
        implicit config: Config,
        ec: ExecutionContext,
        ce: ConcurrentEffect[F],
        t: Timer[F],
        cs: ContextShift[F],
        sf: SchemaFor[T],
        fr: FromRecord[T],
        ct: ClassTag[T]): fs2.Stream[F, Unit] = {
      val sslProperties = kafkaConfig.ssl
        .map { ssl =>
          Map(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG      -> Paths.get(ssl.keystore.location).toAbsolutePath.toString,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG          -> "PKCS12",
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG      -> ssl.keystore.password,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG           -> ssl.keyPassword,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG    -> Paths.get(ssl.truststore.location).toString,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG        -> "JKS",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG    -> ssl.truststore.password
          )
        }
        .getOrElse(Map.empty)

      val schemaRegistryClientSettings = SchemaRegistryClientSettings(
        kafkaConfig.schemaRegistry.url,
        kafkaConfig.schemaRegistry.username,
        kafkaConfig.schemaRegistry.password
      )

      val consumerSettings = (executionContext: ExecutionContext) =>
        ConsumerSettings(
          keyDeserializer = new StringDeserializer,
          valueDeserializer =
            avroBinarySchemaIdDeserializer[T](schemaRegistryClientSettings, isKey = false, includesFormatByte = true),
          executionContext = executionContext
        ).withBootstrapServers(kafkaConfig.hosts)
          .withGroupId(kafkaConfig.groupId)
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withProperties(sslProperties)

      for {
        executionContext <- consumerExecutionContextStream[F]
        consumer         <- consumerStream[F].using(consumerSettings(executionContext))
        _                <- Stream.eval(consumer.subscribe(topics.map(_.name)))
        _ <- consumer
          .partitionedStream
          .parJoinUnbounded
          .mapAsync(25) { message =>
            f(message.record).as(message.committableOffset) // TODO: Use producer stream
          }
          .to(commitBatchWithin(500, 15.seconds))
      } yield ()
    }
  }
}
