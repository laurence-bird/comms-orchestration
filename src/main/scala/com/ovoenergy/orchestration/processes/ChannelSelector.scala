package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.NonEmptyList
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.{Channel, CommManifest, ErrorCode, TriggeredV2}
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.{ErrorsOr, TemplatesContext, TemplatesRepo}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.annotation.tailrec

object ChannelSelector {
  def determineChannel(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]])(
      customerProfile: CustomerProfile,
      triggeredV2: TriggeredV2): Either[ErrorDetails, Channel] = {

    val preferredChannels = triggeredV2.preferredChannels.flatMap(NonEmptyList.fromList)
    val availableChannels = findAvailableChannels(retrieveTemplate, triggeredV2)

    availableChannels.right.map { avChannels =>
      preferredChannels
        .map { prefChans =>
          findPreferredChannel(avChannels, prefChans)
        }
        .getOrElse(determineCheapestChannel(avChannels))
    }
  }

  @tailrec
  private def findPreferredChannel(availableChannels: NonEmptyList[Channel],
                                   preferredChannels: NonEmptyList[Channel]): Channel = {
    val preferencesHead = preferredChannels.head
    val preferencesTail = preferredChannels.tail

    if (availableChannels.exists(_ == preferencesHead))
      preferencesHead
    else if (preferencesTail.nonEmpty)
      findPreferredChannel(availableChannels, NonEmptyList.fromListUnsafe(preferencesTail))
    else determineCheapestChannel(availableChannels)
  }

  private def determineCheapestChannel(availableChannels: NonEmptyList[Channel]): Channel = {
    if (availableChannels.find(_ == Channel.Email).isDefined)
      Email
    else
      SMS
  }

  private def findAvailableChannels(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]],
                                    triggeredV2: TriggeredV2): Either[ErrorDetails, NonEmptyList[Channel]] = {
    retrieveTemplate(triggeredV2.metadata.commManifest) match {
      case Valid(template) =>
        val channelsAvailable: List[Channel] = List(template.email.map(_ => Email), template.sms.map(_ => SMS)).flatten

        NonEmptyList
          .fromList[Channel](channelsAvailable)
          .toRight(
            ErrorDetails(
              s"No Channel found for comm: ${triggeredV2.metadata.commManifest.name} version ${triggeredV2.metadata.commManifest.version}",
              ErrorCode.InvalidTemplate
            )
          )
      case _ =>
        Left(
          ErrorDetails(
            s"No Templates found for comm: ${triggeredV2.metadata.commManifest.name} version ${triggeredV2.metadata.commManifest.version}",
            ErrorCode.InvalidTemplate
          ))
    }
  }

}
