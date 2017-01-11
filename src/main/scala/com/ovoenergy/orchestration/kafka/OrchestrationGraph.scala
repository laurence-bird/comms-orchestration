package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{ErrorCode, Triggered}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.control.NonFatal

case class OrchestrationGraphConfig(hosts: String, groupId: String, topic: String)

object OrchestrationGraph extends LoggingWithMDC {
  override def loggerName: String = "OrchestrationFlow"

  def apply(consumerDeserializer: Deserializer[Option[Triggered]],
            orchestrationProcess: (Triggered) => Either[ErrorDetails, Future[_]],
            failureProcess: (String, Triggered, ErrorCode) => Future[_],
            config: OrchestrationGraphConfig)
            (implicit actorSystem: ActorSystem, materializer: Materializer): RunnableGraph[Control] = {

    implicit val executionContext = actorSystem.dispatcher

    val decider: Supervision.Decider = {
      case NonFatal(e) =>
        log.error("Stopping due to error", e)
        Supervision.Stop
    }

    val consumerSettings =
      ConsumerSettings(actorSystem, new StringDeserializer, consumerDeserializer)
        .withBootstrapServers(config.hosts)
        .withGroupId(config.groupId)

    val source = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(config.topic))
      .mapAsync(1)(msg => {
        log.debug(s"Event received $msg")
        val result: Future[_] = msg.record.value match {
          case Some(triggered) =>
            orchestrationProcess(triggered) match {
              case Left(err) => failureProcess(s"Orchestration failed: ${err.reason}", triggered, err.errorCode)
              case Right(future) => future.recoverWith {
                case NonFatal(error) =>
                  logWarn(triggered.metadata.traceToken, "Orchestration failed, raising failure", error)
                  failureProcess(s"Orchestration failed: ${error.getMessage}", triggered, OrchestrationError)
              }
            }
          case None =>
            log.warn(s"Skipping event: $msg, failed to parse")
            Future.successful(())
        }
        result
          .flatMap(_ => msg.committableOffset.commitScaladsl())
      }).withAttributes(ActorAttributes.supervisionStrategy(decider))

    val sink = Sink.ignore.withAttributes(ActorAttributes.supervisionStrategy(decider))

    source.to(sink)
  }
}
