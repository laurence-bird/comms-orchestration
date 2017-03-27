package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.NonEmptyList
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.{Channel, CommManifest, ErrorCode, TriggeredV2}
import com.ovoenergy.comms.templates.ErrorsOr
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.orchestration.domain.customer.CustomerProfile
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

import scala.annotation.tailrec

object ChannelSelector {
  def determineChannel(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]])(
      customerProfile: CustomerProfile,
      triggered: TriggeredV2): Either[ErrorDetails, Channel] = {

    val channelsWithContactDetails = findChannelsWithContactDetails(customerProfile)
    val channelsWithTemplates      = findChannelsWithTemplates(retrieveTemplate, triggered)

    /*
     Channels with delivery details in the customer profile, matching the trigger preferences
     */
    val channelsAvailableForCustomer: Either[ErrorDetails, List[Channel]] =
      channelsWithTemplates.right.map(_.filter(channelsWithContactDetails.contains))

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

    /*
      Comm preferences specified in the customer account
     */
    priorityOrderedChannelsAvailable.right.map { availableChannels =>
      val customerPreferences = customerProfile.communicationPreferences
        .find(_.commType == triggered.metadata.commManifest.commType)
        .map(_.channels)
        .getOrElse(Nil)
        .toList

      /*
        Preferred channel for customer
       */
      if (customerPreferences.isEmpty)
        availableChannels.head
      else
        findPreferredChannel(availableChannels, customerPreferences)
    }
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
    println("Cheapest channel being found yo")
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

  private def findChannelsWithContactDetails(customerProfile: CustomerProfile): List[Channel] = {
    List(
      customerProfile.phoneNumber.map(_ => SMS),
      customerProfile.emailAddress.map(_ => Email)
    ).flatten
  }

}
