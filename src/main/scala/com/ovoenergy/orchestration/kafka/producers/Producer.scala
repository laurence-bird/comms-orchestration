package com.ovoenergy.orchestration.kafka.producers

import cats.effect.Async
import cats.implicits._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

case class KafkaTopic(name: String)

class Publisher[T](producer: Producer[String, T],
                   topic: KafkaTopic) extends LoggingWithMDC{

  def mdcParametersFor(rm: RecordMetadata) = {
    Map(
      "kafkaTopic" -> rm.topic(),
      "kafkaPartition" -> rm.partition().toString,
      "kafkaOffset" -> rm.offset().toString
    )
  }

  def publish[F[_]: Async](t: T, key: String): F[Either[String, Unit]] = {
    val record = new ProducerRecord[String, T](
      topic.name,
      key,
      t
    )

    val f: F[RecordMetadata] = Async[F].async[RecordMetadata] { cb =>
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata,
                                    exception: Exception): Unit = {
            Option(exception).fold(cb(Right(metadata)))(e => cb(Left(e)))
          }
        }
      )

      ()
    }

    f.flatMap[Either[String, Unit]] { rm: RecordMetadata =>
      Async[F].delay(logInfo(mdcParametersFor(rm), "Triggered produced")) >> Async[F].pure(
        Right(()))
    }
      .recover {
        case e => Left(e.getMessage)
      }
  }

}
