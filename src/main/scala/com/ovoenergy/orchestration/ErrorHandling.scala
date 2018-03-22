package com.ovoenergy.orchestration

import akka.actor.ActorSystem
import cats.effect.Async
import com.ovoenergy.comms.helpers.{EventLogger, HasCommName, Topic}
import com.ovoenergy.comms.model.TriggeredV3
import com.ovoenergy.comms.serialisation.Retry
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.concurrent.Future
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

  def retryPublisherFor[E](topic: Topic[E])(implicit schemaFor: SchemaFor[E],
                                            toRecord: ToRecord[E],
                                            classTag: ClassTag[E],
                                            eventLogger: EventLogger[E],
                                            hasCommName: HasCommName[E],
                                            actorSystem: ActorSystem): (E) => Future[RecordMetadata] = {

    exitAppOnFailure(topic.retryPublisher, topic.name)
  }
}
