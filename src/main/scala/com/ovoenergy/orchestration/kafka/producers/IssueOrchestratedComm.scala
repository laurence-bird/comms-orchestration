package com.ovoenergy.orchestration.kafka.producers

import cats.effect.{Async, IO}
import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV4}
import com.ovoenergy.orchestration.domain.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

abstract class IssueOrchestratedComm[A <: ContactInfo] {
  def send(customerProfile: Option[CustomerProfile], contactInfo: A, triggered: TriggeredV4): IO[RecordMetadata]
}
