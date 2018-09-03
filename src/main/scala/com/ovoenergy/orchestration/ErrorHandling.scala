package com.ovoenergy.orchestration

import cats.effect.{Async, IO}
import cats.syntax.flatMap._
import com.ovoenergy.comms.helpers.{EventLogger, HasCommName, Topic}
import com.ovoenergy.comms.model.LoggableEvent
import com.ovoenergy.comms.serialisation.Retry
import com.ovoenergy.orchestration.kafka.Producer
import com.ovoenergy.orchestration.logging.{Loggable, LoggingWithMDC}
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata}

import scala.reflect.ClassTag

object ErrorHandling extends LoggingWithMDC {

  case class ErrorToLog(str: String, exception: Throwable)

  trait ErrorBuilder[T] {
    def buildError(topic: String): T => ErrorToLog
  }

  // This is only being used for calls to the schema registry currently, hence the error message
  implicit val retryFailureBuilder = new ErrorBuilder[Retry.Failed] {
    override def buildError(topic: String): (Retry.Failed) => ErrorToLog = { failed =>
      ErrorToLog(
        s"Failed to register schema to topic $topic, ${failed.attemptsMade} attempts made",
        failed.finalException
      )
    }
  }

  def exitAppOnFailure[LeftRes, RightRes](eitherval: Either[LeftRes, RightRes], topicName: String)(
      implicit errorBuidler: ErrorBuilder[LeftRes]): RightRes = {
    eitherval match {
      case Left(err) => {
        val loggableError = errorBuidler.buildError(topicName).apply(err)
        log.error(loggableError.str, loggableError.exception)
        sys.exit(1)
      }
      case Right(res) => res
    }
  }
}
