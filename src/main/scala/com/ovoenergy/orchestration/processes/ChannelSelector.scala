package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.NonEmptyList
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.InvalidProfile
import com.ovoenergy.comms.model.{Channel, CommManifest, ErrorCode, TriggeredV2}
import com.ovoenergy.comms.templates.ErrorsOr
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.annotation.tailrec

object ChannelSelector extends LoggingWithMDC {
  def determineChannel(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]])(
      customerProfile: CustomerProfile,
      triggered: TriggeredV2): Either[ErrorDetails, Channel] = {

    /*
     Channels with delivery details in the customer profile, and available templates
     */
    val channelsAvailableForCustomer = for {
      channelsWithContactDetails <- findChannelsWithContactDetails(customerProfile).right
      channelsWithTemplates      <- findChannelsWithTemplates(retrieveTemplate, triggered).right
    } yield channelsWithTemplates.filter(channel => channelsWithContactDetails.exists(_ == channel))

    /*
      Channels available, in priority order according to trigger preferences
     */
    val priorityOrderedChannelsAvailable = triggered.preferredChannels
      .map { triggerPreferences =>
        channelsAvailableForCustomer.right.map { avChannels =>
          triggerPreferences.filter((t: Channel) => avChannels.contains(t))
        }
      }
      .getOrElse(channelsAvailableForCustomer)

    val result = priorityOrderedChannelsAvailable.right.map { availableChannels =>
      // Comm preferences specified in the customer account
      val customerPreferences = customerProfile.communicationPreferences
        .find(_.commType == triggered.metadata.commManifest.commType)
        .map(_.channels)
        .getOrElse(Nil)
        .toList

      /*
        Preferred channel for customer based on customer preferences and the above specifications
       */
      if (customerPreferences.isEmpty)
        availableChannels.head
      else
        findPreferredChannel(availableChannels, customerPreferences)
    }
    logInfo(triggered.metadata.traceToken, s"Channel retrieved result yo :$result")
    result
  }

  @tailrec
  private def findPreferredChannel(availableChannels: List[Channel], preferredChannels: List[Channel]): Channel = {
    val availableChannel = availableChannels.head
    if (preferredChannels.contains(availableChannel))
      availableChannel
    else if (preferredChannels.length > 1)
      findPreferredChannel(availableChannels, preferredChannels.tail)
    else
      determineCheapestChannel(availableChannels)
  }

  private def determineCheapestChannel(availableChannels: List[Channel]) = {
    if (availableChannels.contains(Channel.Email))
      Email
    else if (availableChannels.contains(Channel.SMS))
      SMS
    else
      availableChannels.head
  }

  private def findChannelsWithTemplates(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]],
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

  private def findChannelsWithContactDetails(
      customerProfile: CustomerProfile): Either[ErrorDetails, NonEmptyList[Channel]] = {
    val channels = List(
      customerProfile.phoneNumber.map(_ => SMS),
      customerProfile.emailAddress.map(_ => Email)
    ).flatten

    NonEmptyList
      .fromList(channels)
      .toRight(Orchestrator.ErrorDetails(s"No contact details found on customer profile", InvalidProfile))
  }
}
