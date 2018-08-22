package com.ovoenergy.comms.orchestration.kafka

import cats.effect.Async
import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV4}
import com.ovoenergy.comms.orchestration.domain.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

abstract class IssueOrchestratedComm[A <: ContactInfo, F[_]: Async] {
  def send(customerProfile: Option[CustomerProfile], contactInfo: A, triggered: TriggeredV4): F[RecordMetadata]
}
