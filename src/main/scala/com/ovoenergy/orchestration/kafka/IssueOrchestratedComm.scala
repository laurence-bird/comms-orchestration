package com.ovoenergy.orchestration.kafka

import com.ovoenergy.comms.model.{CustomerProfile, TriggeredV3}
import com.ovoenergy.orchestration.domain.customer.ContactInfo
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.Future

trait IssueOrchestratedComm[A <: ContactInfo] {
  def send(customerProfile: Option[CustomerProfile], contactInfo: A, triggered: TriggeredV3): Future[RecordMetadata]
}
