package com.ovoenergy.orchestration.kafka.producers

import cats.effect.Async
import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV4}
import com.ovoenergy.orchestration.domain.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

trait IssueOrchestratedComm[F[_], A] {
  def send(customerProfile: Option[CustomerProfile], contactInfo: A, triggered: TriggeredV4): F[RecordMetadata]
}
