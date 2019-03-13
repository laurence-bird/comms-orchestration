package com.ovoenergy.orchestration
package kafka
package producers

import java.nio.file.Paths
import java.util.UUID

import cats.effect.{Async, IO, Timer}
import cats.syntax.flatMap._

import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer

import com.ovoenergy.kafka.serialization.avro4s._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object Producer extends LoggingWithMDC {

  def apply[E: SchemaFor: ToRecord](config: Config.Kafka): KafkaProducer[String, E] = {

    val producerSettings: Map[String, AnyRef] = config.producer.mapValues(x => x: AnyRef)

    val serializer = avroBinarySchemaIdSerializer[E](config.schemaRegistry, isKey = false, includesFormatByte = true)
    new KafkaProducer[String, E](producerSettings.asJava, new StringSerializer(), serializer)
  }

  def publisher[E, F[_]](getKey: E => String, producer: KafkaProducer[String, E], topicName: String)(
      t: E)(implicit ec: ExecutionContext = ExecutionContext.global, F: Async[F]) = {

    val record = new ProducerRecord[String, E](
      topicName,
      getKey(t),
      t
    )

    F.async[RecordMetadata] { cb =>
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            Option(exception).fold(cb(Right(metadata)))(e => cb(Left(e)))
          }
        }
      )
    } // TODO: Confirm if shift was working previously, pass in blocking ec potentially (though everything is blocking anyway)
  }

  def publisherFor[E: Loggable, F[_]](config: Config.Kafka, topic: Config.Topic[E], key: E => String)(
      implicit schemaFor: SchemaFor[E],
      toRecord: ToRecord[E],
      classTag: ClassTag[E],
      F: Async[F]): E => F[RecordMetadata] = {
    val producer: KafkaProducer[String, E] = Producer[E](config)
    val pub: E => F[RecordMetadata] = { e: E =>
      publisher[E, F](key, producer, topic.value)(e)
        .flatTap((rm: RecordMetadata) => F.delay(info((rm, e))(s"Sent event to ${topic.value}")))
    }
    pub
  }
}
