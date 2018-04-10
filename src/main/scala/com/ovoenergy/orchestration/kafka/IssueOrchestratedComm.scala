package com.ovoenergy.orchestration.kafka

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV3}
import com.ovoenergy.orchestration.domain.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

abstract class IssueOrchestratedComm[A <: ContactInfo, F[_]: Async] {
  def send(customerProfile: Option[CustomerProfile], contactInfo: A, triggered: TriggeredV3): F[RecordMetadata]
}
