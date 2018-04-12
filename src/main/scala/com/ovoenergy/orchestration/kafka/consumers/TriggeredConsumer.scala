package com.ovoenergy.orchestration.kafka.consumers

import cats.effect.{Async}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.processes.TriggeredDataValidator
import cats.syntax.flatMap._

import scala.concurrent.{ExecutionContext, Future}

object TriggeredConsumer extends LoggingWithMDC {

  def apply[F[_]: Async](scheduleTask: (TriggeredV3) => F[Either[ErrorDetails, Boolean]],
                         sendFailedEvent: FailedV2 => F[Unit],
                         generateTraceToken: () => String)(implicit ec: ExecutionContext): TriggeredV3 => F[Unit] = {
    triggeredV3: TriggeredV3 =>

      def scheduleOrFail(triggeredV3: TriggeredV3) = {
        scheduleTask(triggeredV3) flatMap {
          case Right(r)  => Async[F].pure(())
          case Left(err) => sendFailedEvent(failedEventFromErr(err))
        }
      }

      def failedEventFromErr(err: ErrorDetails): FailedV2 = {
        FailedV2(
          MetadataV2.fromSourceMetadata("orchestration", triggeredV3.metadata),
          InternalMetadata(generateTraceToken()),
          err.reason,
          err.errorCode
        )
      }

      TriggeredDataValidator(triggeredV3) match {
        case Right(t) => scheduleOrFail(t)
        case Left(err) =>
          sendFailedEvent(failedEventFromErr(err))
      }
  }
}
