package com.ovoenergy.orchestration
package kafka
package consumers

import java.nio.file.Paths

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings
import com.ovoenergy.kafka.serialization.avro4s._

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

    def apply[T](config: Config.Kafka, topics: NonEmptyList[Config.Topic[T]])(
        pollTimeout: FiniteDuration = DefaultPollTimeout)(f: Record[T] => F[Unit])(
        implicit ec: ExecutionContext,
        ce: ConcurrentEffect[F],
        t: Timer[F],
        cs: ContextShift[F],
        sf: SchemaFor[T],
        fr: FromRecord[T],
        ct: ClassTag[T]): fs2.Stream[F, Unit] = {

      val consumerSettings = (executionContext: ExecutionContext) =>
        ConsumerSettings(
          keyDeserializer = new StringDeserializer,
          valueDeserializer =
            avroBinarySchemaIdDeserializer[T](config.schemaRegistry, isKey = false, includesFormatByte = true),
          executionContext = executionContext
        ).withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withProperties(config.consumer)

      for {
        executionContext <- consumerExecutionContextStream[F]
        consumer         <- consumerStream[F].using(consumerSettings(executionContext))
        _                <- Stream.eval(consumer.subscribe(topics.map(_.value)))
        _ <- consumer.partitionedStream.parJoinUnbounded
          .mapAsync(25) { message =>
            f(message.record).as(message.committableOffset)
          }
          .through(commitBatchWithin(500, 15.seconds))
      } yield ()
    }
  }
}
