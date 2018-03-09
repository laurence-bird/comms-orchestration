package com.ovoenergy.orchestration.processes

import cats.Monoid
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.ovoenergy.comms.model.{OrchestrationError, TemplateData, TriggeredV3}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import shapeless.{Inl, Inr}

object TriggeredDataValidator extends LoggingWithMDC {

  implicit val booleanMonoid: Monoid[Boolean] = new Monoid[Boolean]{
    override def empty = true
    override def combine(x: Boolean, y: Boolean) = x && y
  }

  def apply(triggered: TriggeredV3): Either[ErrorDetails, TriggeredV3] = {

    val fields = List(
      checkIfEmpty(triggered.metadata.traceToken, "traceToken"),
      checkIfEmpty(triggered.metadata.eventId, "eventId"),
      checkIfEmpty(triggered.metadata.friendlyDescription, "friendlyDescription"),
      checkIfEmpty(triggered.metadata.source, "source"),
      checkIfEmpty(triggered.metadata.triggerSource, "triggerSource"),
      checkIfEmpty(triggered.metadata.commManifest.name, "commManifestName"),
      checkIfEmpty(triggered.metadata.commManifest.version, "commManifestVersion"),
    )

    val templateData = triggered.templateData.map(entry => checkTemplateData(s"templateData.${entry._1}", entry._2))

    fields ++ templateData foldMap(identity) match {
      case Valid(_) => Right(triggered)
      case Invalid(emptyFields) =>
        Left(ErrorDetails(
          s"The following fields contain empty string: ${emptyFields.toList.mkString(", ")}",
          OrchestrationError)
        )
    }

  }

  private def checkIfEmpty(value: String, name: String): ValidatedNel[String, Boolean] =
    if(value.isEmpty || name.isEmpty) {
      Invalid(NonEmptyList.of(name))
    } else {
      Valid(true)
    }

  private def checkTemplateData(key: String, td: TemplateData): ValidatedNel[String, Boolean] = td.value match {
    case (Inl(stringValue: String)) => checkIfEmpty(stringValue, key)
    case (Inr(Inl(sequence)))       => sequence.toList.foldMap(t => checkTemplateData(key, t))
    case (Inr(Inr(Inl(mapObj))))    => mapObj.map(e => checkTemplateData(s"$key.${e._1}", e._2)).toList.foldMap(identity)
    case (Inr(Inr(Inr(_))))         => Invalid(NonEmptyList.of(s"Unable to extract value from templateData.$key"))
  }

}
