package com.ovoenergy.orchestration.kafka.consumers
import java.nio.file.Paths

import cats.data.NonEmptyList
import cats.effect._
import com.ovoenergy.comms.helpers.{Kafka, Topic}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, reflectiveCalls}
import scala.concurrent.duration._

import com.sksamuel.avro4s.{FromRecord, SchemaFor}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag

object KafkaConsumer {

  type Record[T] = ConsumerRecord[Unit, T]

  final case class Settings[T](deserialiser: Deserializer[Option[T]])
  val DefaultPollTimeout: FiniteDuration = 150.milliseconds

  def apply[F[_]] = new ApplyPartiallyApplied[F]

  class ApplyPartiallyApplied[F[_]] {

    def apply[A, T](topics: NonEmptyList[Topic[T]])(pollTimeout: FiniteDuration = DefaultPollTimeout)(
        f: Record[T] => F[A])(implicit config: Config,
                              ec: ExecutionContext,
                              F: Effect[F],
                              sf: SchemaFor[T],
                              fr: FromRecord[T],
                              ct: ClassTag[T]): fs2.Stream[F, A] = {
//
//      val deserialiser: Deserializer[T] = topics.head.deserializer
//      val aivenCluster                  = Kafka.aiven
//      val kafkaClusterConfig            = aivenCluster.kafkaConfig
//
//      val consumerNativeSettings: Map[String, AnyRef] = {
//        Map(
//          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG    -> kafkaClusterConfig.hosts,
//          ConsumerConfig.GROUP_ID_CONFIG             -> kafkaClusterConfig.groupId,
//          ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "600000", // 10 minutes
//          ConsumerConfig.MAX_POLL_RECORDS_CONFIG     -> "100" // 10 minutes
//        ) ++ kafkaClusterConfig.ssl
//          .map { ssl =>
//            Map(
//              CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
//              SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG      -> Paths.get(ssl.keystore.location).toAbsolutePath.toString,
//              SslConfigs.SSL_KEYSTORE_TYPE_CONFIG          -> "PKCS12",
//              SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG      -> ssl.keystore.password,
//              SslConfigs.SSL_KEY_PASSWORD_CONFIG           -> ssl.keyPassword,
//              SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG    -> Paths.get(ssl.truststore.location).toString,
//              SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG        -> "JKS",
//              SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG    -> ssl.truststore.password
//            )
//          }
//          .getOrElse(Map.empty) ++ kafkaClusterConfig.nativeProperties
//      }
//
//      val consumerSettings: ConsumerSettings = ConsumerSettings(
//        pollTimeout = pollTimeout,
//        maxParallelism = Int.MaxValue,
//        nativeSettings = consumerNativeSettings
//      )
//
//      consumeProcessAndCommit[F].apply(
//        Subscription.topics(topics.map(_.name)),
//        constDeserializer[Unit](()),
//        deserialiser,
//        consumerSettings
//      )(f)
      ???
    }
  }
}
