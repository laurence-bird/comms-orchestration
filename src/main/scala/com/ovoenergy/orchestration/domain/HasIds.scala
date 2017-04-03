package com.ovoenergy.orchestration.domain

import com.ovoenergy.comms.model._

trait HasIds[A] {
  def traceToken(a: A): String
  def customerId(a: A): String
}

object HasIds {
  def instance[A](getTraceToken: A => String, getCustomerId: A => String): HasIds[A] = new HasIds[A] {
    def traceToken(a: A) = getTraceToken(a)
    def customerId(a: A) = getCustomerId(a)
  }

  implicit val failedCancellationHasIds = HasIds
    .instance[FailedCancellation](_.cancellationRequested.metadata.traceToken, _.cancellationRequested.customerId)
  implicit val failedHasIds      = HasIds.instance[Failed](_.metadata.traceToken, _.metadata.customerId)
  implicit val orchStartedHasIds = HasIds.instance[OrchestrationStarted](_.metadata.traceToken, _.metadata.customerId)
  implicit val cancelledHasIds   = HasIds.instance[Cancelled](_.metadata.traceToken, _.metadata.customerId)
  implicit val OrchestratedEmailHasIds =
    HasIds.instance[OrchestratedEmailV2](_.metadata.traceToken, _.metadata.customerId)
  implicit val OrchestratedSMSHasIds =
    HasIds.instance[OrchestratedSMS](_.metadata.traceToken, _.metadata.customerId)
}
