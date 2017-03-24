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

    val preferredChannels          = triggeredV2.preferredChannels.flatMap(NonEmptyList.fromList)
    val channelsWithTemplates      = findAvailableChannels(retrieveTemplate, triggeredV2)
    val channelsWithContactDetails = findChannelsWithContactDetails(customerProfile)

    val availableChannels = channelsWithTemplates.right.map(channels =>
      channelsWithContactDetails.filterNot(yolo => channels.exists(_ == yolo)))

    val availableChannelsNel = availableChannels.right.flatMap(avChans =>
      NonEmptyList.fromList(avChans).toRight(ErrorDetails("yolo", ErrorCode.OrchestrationError)))

    availableChannelsNel.right.map { (availableChans: NonEmptyList[Channel]) =>
      preferredChannels
        .map(triggerChannelPreferences => findPreferredChannel(availableChans, triggerChannelPreferences))
        .getOrElse(determineCheapestChannel(availableChans))
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

  private def findChannelsWithContactDetails(customerProfile: CustomerProfile): List[Channel] = {
    List(
      customerProfile.phoneNumber.map(_ => SMS),
      customerProfile.emailAddress.map(_ => Email)
    ).flatten
  }

}
