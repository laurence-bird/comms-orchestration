package com.ovoenergy.orchestration.kafka

import cats.effect.Async
import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV3}
import com.ovoenergy.orchestration.domain.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

trait IssueOrchestratedComm[A <: ContactInfo] {
  def send[F[_]: Async](customerProfile: Option[CustomerProfile],
                        contactInfo: A,
                        triggered: TriggeredV3): F[Either[String, Unit]]
}
