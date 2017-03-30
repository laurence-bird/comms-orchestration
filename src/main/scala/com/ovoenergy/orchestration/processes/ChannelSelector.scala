package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.NonEmptyList
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel.{Email, SMS}
import com.ovoenergy.comms.model.ErrorCode.{InvalidProfile, InvalidTemplate, OrchestrationError}
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
    val channelsAvailableForCustomer: Either[ErrorDetails, NonEmptyList[Channel]] = for {
      channelsWithContactDetails <- findChannelsWithContactDetails(customerProfile).right
      channelsWithTemplates      <- findChannelsWithTemplates(retrieveTemplate, triggered).right
      availableChannels  <- {
        val avChans = channelsWithTemplates.filter(channel => channelsWithContactDetails.exists(_ == channel))
        nonEmptyListFrom(avChans, "No available channels to deliver comm", OrchestrationError).right
      }
    } yield availableChannels


    /*
      Channels available, in priority order according to trigger preferences
     */
    val priorityOrderedChannelsAvailable = triggered.preferredChannels
      .map { triggerPreferences =>
        channelsAvailableForCustomer.right.flatMap {
          availableChannels =>
            val otherChannels = triggerPreferences
              .filter(preferredChannel=> availableChannels.exists(_ == preferredChannel))
            nonEmptyListFrom(otherChannels, "No preferred channels available", OrchestrationError)
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
    result
  }

  private def nonEmptyListFrom[A](list: List[A], errorMessage: String, errorCode: ErrorCode): Either[ErrorDetails, NonEmptyList[A]] = {
    NonEmptyList.fromList(list)
        .toRight(ErrorDetails(errorMessage, errorCode))
  }

  @tailrec
  private def findPreferredChannel(availableChannels: NonEmptyList[Channel],
                                   preferredChannels: List[Channel]): Channel = {
    val availableChannel = availableChannels.head
    if (preferredChannels.contains(availableChannel))
      availableChannel
    else if (preferredChannels.length > 1)
      findPreferredChannel(availableChannels, preferredChannels.tail)
    else
      determineCheapestChannel(availableChannels)
  }

  private def determineCheapestChannel(availableChannels: NonEmptyList[Channel]) = {
    if (availableChannels.find(_ == Channel.Email).isDefined)
      Email
    else if (availableChannels.find(_ == Channel.SMS).isDefined)
      SMS
    else
      availableChannels.head
  }

  private def findChannelsWithTemplates(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]],
                                        triggeredV2: TriggeredV2): Either[ErrorDetails, NonEmptyList[Channel]] = {
    retrieveTemplate(triggeredV2.metadata.commManifest) match {
      case Valid(template) =>
        val channelsWithTemplates = List(template.email.map(_ => Email), template.sms.map(_ => SMS)).flatten

        nonEmptyListFrom(channelsWithTemplates, s"No valid template found for comm: ${triggeredV2.metadata.commManifest.name} version ${triggeredV2.metadata.commManifest.version}", InvalidTemplate)
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

    nonEmptyListFrom(channels, "No contact details found on customer profile", InvalidProfile)
  }
}
