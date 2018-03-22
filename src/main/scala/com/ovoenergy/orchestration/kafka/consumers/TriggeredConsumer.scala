package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.Async
import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.TriggeredDataValidator
import org.apache.kafka.clients.producer.RecordMetadata
import cats.syntax.functor._
import scala.concurrent.Future

object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](
      topic: Topic[TriggeredV3],
      scheduleTask: (TriggeredV3) => Either[ErrorDetails, Boolean],
      sendFailedEvent: FailedV2 => F[RecordMetadata],
      generateTraceToken: () => String)(triggeredV3: TriggeredV3): F[Either[RecordMetadata, Unit]] = {

    val result = TriggeredDataValidator(triggeredV3).flatMap(scheduleTask)

    result match {
      case Left(err) =>
        sendFailedEvent(
          FailedV2(
            MetadataV2.fromSourceMetadata("orchestration", triggeredV3.metadata),
            InternalMetadata(generateTraceToken()),
            err.reason,
            err.errorCode
          )
        ).map(r => Left(r))
      case Right(_) => Async[F].pure(Right(()))
    }
  }
}
