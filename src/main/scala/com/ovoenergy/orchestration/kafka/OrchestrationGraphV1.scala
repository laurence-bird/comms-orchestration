package com.ovoenergy.orchestration.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{RunnableGraph, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.ovoenergy.comms.model.ErrorCode.OrchestrationError
import com.ovoenergy.comms.model.{ErrorCode, InternalMetadata, Triggered, TriggeredV2}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.control.NonFatal


object OrchestrationGraphV1 extends LoggingWithMDC {
  override def loggerName: String = "OrchestrationGraphV1"

  def apply(consumerDeserializer: Deserializer[Option[Triggered]],
            triggeredConverter: Triggered => TriggeredV2,
            orchestrationProcess: (TriggeredV2, InternalMetadata) => Either[ErrorDetails, Future[_]],
            failureProcess: (String, TriggeredV2, ErrorCode, InternalMetadata) => Future[_],
            config: SchedulingGraphConfig,
            traceTokenGenerator: () => String)
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
        val internalMetaData = InternalMetadata(traceTokenGenerator.apply)
        val result: Future[_] = msg.record.value match {
          case Some(triggered) =>
            val triggeredV2 = triggeredConverter(triggered)
            orchestrationProcess(triggeredV2, internalMetaData) match {
              case Left(err) => failureProcess(s"Orchestration failed: ${err.reason}", triggeredV2, err.errorCode, internalMetaData)
              case Right(future) => future.recoverWith {
                case NonFatal(error) =>
                  logWarn(triggeredV2.metadata.traceToken, "Orchestration failed, raising failure", error)
                  failureProcess(s"Orchestration failed: ${error.getMessage}", triggeredV2, OrchestrationError, internalMetaData)
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
