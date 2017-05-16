package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.NonEmptyList
import cats.syntax.either._
import cats.data.Validated.{Invalid, Valid}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.ErrorsOr
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.orchestration.domain.customer.{CommunicationPreference, CustomerProfile}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

object ChannelSelector extends LoggingWithMDC {

  private val channelCostMap: Map[Channel, Int] = Map(
    Email -> 1,
    SMS   -> 2
  )

  private val costPriority = priority(channelCostMap) _

  private def priority(priorityMap: Map[Channel, Int])(channel: Channel) = priorityMap.getOrElse(channel, Int.MaxValue)

  def determineChannel(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]])(
      customerProfile: CustomerProfile,
      triggered: TriggeredV3): Either[ErrorDetails, Channel] = {

    val customerPrefs: Option[NonEmptyList[Channel]] = {
      val prefsForCommType = customerProfile.communicationPreferences.collectFirst({
        case CommunicationPreference(commType, prefs) if commType == triggered.metadata.commManifest.commType => prefs
      })
      prefsForCommType.flatMap { prefs =>
        val prefsForSupportedChannels = prefs.toList.filter(channel => channelCostMap.contains(channel))
        NonEmptyList.fromList(prefsForSupportedChannels)
      }
    }

    def findAvailableChannels(
        channelsWithTemplates: NonEmptyList[Channel],
        channelsWithContactDetails: NonEmptyList[Channel]): Either[ErrorDetails, NonEmptyList[Channel]] = {
      val avChans = channelsWithTemplates.toList.intersect(channelsWithContactDetails.toList)
      nonEmptyListFrom(avChans, "No available channels to deliver comm", OrchestrationError)
    }

    def filterByCustomerPreferences(
        availableChannels: NonEmptyList[Channel],
        customerPreferences: Option[NonEmptyList[Channel]]): Either[ErrorDetails, NonEmptyList[Channel]] = {
      customerPreferences match {
        case Some(nel) =>
          val filteredChannels = availableChannels.toList.intersect(nel.toList)
          nonEmptyListFrom(filteredChannels, "No available channels that the customer accepts", OrchestrationError)
        case None => Right(availableChannels)
      }
    }

    def determineChannel(availableChannels: NonEmptyList[Channel]): Channel = {
      val triggerPreferencesMap = triggered.preferredChannels.getOrElse(Nil).zipWithIndex.toMap
      val triggerPriority       = priority(triggerPreferencesMap) _

      availableChannels.toList
        .sortBy(costPriority)
        .sortBy(triggerPriority)
        .head
    }

    for {
      channelsWithContactDetails <- findChannelsWithContactDetails(customerProfile)
      channelsWithTemplates      <- findChannelsWithTemplates(retrieveTemplate, triggered)
      availableChannels          <- findAvailableChannels(channelsWithTemplates, channelsWithContactDetails)
      acceptableChannels         <- filterByCustomerPreferences(availableChannels, customerPrefs)
    } yield determineChannel(acceptableChannels)

  }

  private def nonEmptyListFrom[A](list: List[A],
                                  errorMessage: String,
                                  errorCode: ErrorCode): Either[ErrorDetails, NonEmptyList[A]] = {
    NonEmptyList
      .fromList(list)
      .toRight(ErrorDetails(errorMessage, errorCode))
  }

  private def findChannelsWithTemplates(retrieveTemplate: CommManifest => ErrorsOr[CommTemplate[Id]],
                                        triggeredV3: TriggeredV3): Either[ErrorDetails, NonEmptyList[Channel]] = {
    retrieveTemplate(triggeredV3.metadata.commManifest) match {
      case Valid(template) =>
        val channelsWithTemplates = List(template.email.map(_ => Email), template.sms.map(_ => SMS)).flatten

        nonEmptyListFrom(
          channelsWithTemplates,
          s"No valid template found for comm: ${triggeredV3.metadata.commManifest.name} version ${triggeredV3.metadata.commManifest.version}",
          InvalidTemplate
        )
      case Invalid(errors) => {
        logInfo(triggeredV3.metadata.traceToken, s"Invalid template retrieved: ${errors.toList.mkString(", ")}")
        Left(
          ErrorDetails(
            s"Invalid template: ${errors.toList.mkString(", ")}",
            InvalidTemplate
          ))
      }
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
