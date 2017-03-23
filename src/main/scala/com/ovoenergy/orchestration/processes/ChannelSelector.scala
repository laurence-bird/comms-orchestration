package com.ovoenergy.orchestration.processes

import cats.data.NonEmptyList
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.{Channel, ErrorCode, TriggeredV2}
import com.ovoenergy.comms.templates.{TemplatesContext, TemplatesRepo}
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.annotation.tailrec

object ChannelSelector {
  def determineChannel(templateContext: TemplatesContext)(customerProfile: CustomerProfile,
                                                          triggeredV2: TriggeredV2): Either[ErrorDetails, Channel] = {
    val preferredChannels = triggeredV2.preferredChannels.getOrElse(Nil)
    val availableChannels = findAvailableChannels(templateContext, triggeredV2)

    availableChannels.right.map(avChannels => findPreferredChannel(avChannels, preferredChannels))
  }

  @tailrec
  private def findPreferredChannel(availableChannels: NonEmptyList[Channel],
                                   preferredChannels: List[Channel]): Channel = {
    val preferencesHead = preferredChannels.head
    val preferencesTail = preferredChannels.tail

    if (availableChannels.exists(_ == preferencesHead))
      preferencesHead
    else if (preferencesTail.nonEmpty)
      findPreferredChannel(availableChannels, preferencesTail)
    else determineCheapestChannel(availableChannels)
  }

  private def determineCheapestChannel(availableChannels: NonEmptyList[Channel]): Channel = {
    if (availableChannels.find(_ == Channel.Email).isDefined)
      Email
    else
      SMS
  }

  private def findAvailableChannels(templateContext: TemplatesContext,
                                    triggeredV2: TriggeredV2): Either[ErrorDetails, NonEmptyList[Channel]] = {
    TemplatesRepo.getTemplate(templateContext, triggeredV2.metadata.commManifest) match {
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
