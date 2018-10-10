package com.ovoenergy.orchestration.processes

import cats.data.{EitherT, NonEmptyList}
import cats.effect.Async
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.ErrorsOr
import com.ovoenergy.orchestration.domain.{CommunicationPreference, ContactProfile}
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.templates.RetrieveTemplateDetails.TemplateDetails
import cats.implicits._

abstract class ChannelSelector[F[_]: Async] {
  def determineChannel(contactProfile: ContactProfile,
                       customerPreferences: Seq[CommunicationPreference],
                       triggered: TriggeredV4): F[Either[ErrorDetails, Channel]]
}

class ChannelSelectorWithTemplate[F[_]: Async](
    retrieveTemplateDetails: TemplateManifest => F[ErrorsOr[TemplateDetails]])
    extends ChannelSelector
    with LoggingWithMDC {

  private val channelCostMap: Map[Channel, Int] = Map(
    Email -> 1,
    SMS   -> 2,
    Print -> 3
  )

  private val costPriority = priority(channelCostMap) _

  private def priority(priorityMap: Map[Channel, Int])(channel: Channel) = priorityMap.getOrElse(channel, Int.MaxValue)

  def determineChannel(contactProfile: ContactProfile,
                       customerPreferences: Seq[CommunicationPreference],
                       triggered: TriggeredV4): F[Either[ErrorDetails, Channel]] = {

    def getTemplateDetails(manifest: TemplateManifest): F[Either[ErrorDetails, TemplateDetails]] = {
      retrieveTemplateDetails(manifest)
        .map(_.toEither.leftMap { errors =>
          ErrorDetails(s"Invalid template: ${errors.toList.mkString(", ")}", InvalidTemplate)
        })
    }

    def filterByCustomerPreferences(
        availableChannels: NonEmptyList[Channel],
        customerPreferences: Option[NonEmptyList[Channel]]): F[Either[ErrorDetails, NonEmptyList[Channel]]] = {
      customerPreferences match {
        case Some(nel) =>
          val filteredChannels = availableChannels.toList.intersect(nel.toList)
          Async[F].pure(
            nonEmptyListFrom(filteredChannels, "No available channels that the customer accepts", OrchestrationError))
        case None => Async[F].pure(Right(availableChannels))
      }
    }

    // doesn't take into account templateDetails
    def customerPrefs(templateDetails: TemplateDetails): Option[NonEmptyList[Channel]] = {
      val prefsForCommType = customerPreferences.collectFirst({
        case CommunicationPreference(commType, prefs) if commType == templateDetails.commType => prefs
      })
      prefsForCommType.flatMap { prefs =>
        val prefsForSupportedChannels = prefs.toList.filter(channel => channelCostMap.contains(channel))
        NonEmptyList.fromList(prefsForSupportedChannels)
      }
    }

    val channelOrError: EitherT[F, ErrorDetails, Channel] = for {
      channelsWithContactDetails <- EitherT(findChannelsWithContactDetails(contactProfile))
      templateDetails            <- EitherT(getTemplateDetails(triggered.metadata.templateManifest))
      customerPs = customerPrefs(templateDetails)
      channelsWithTemplates <- EitherT(findChannelsWithTemplates(triggered, templateDetails))
      availableChannels     <- EitherT(findAvailableChannels(channelsWithTemplates, channelsWithContactDetails))
      acceptableChannels    <- EitherT(filterByCustomerPreferences(availableChannels, customerPs))
    } yield {
      val res = determinePrioritisedChannel(acceptableChannels, triggered.preferredChannels)
      info(triggered)(s"Channel determined for comm: $res")
      res
    }

    channelOrError.value
  }

  private def findAvailableChannels(
      channelsWithTemplates: NonEmptyList[Channel],
      channelsWithContactDetails: NonEmptyList[Channel]): F[Either[ErrorDetails, NonEmptyList[Channel]]] = {
    val avChans = channelsWithTemplates.toList.intersect(channelsWithContactDetails.toList)
    Async[F].pure(nonEmptyListFrom(avChans, "No available channels to deliver comm", OrchestrationError))
  }

  private def determinePrioritisedChannel(availableChannels: NonEmptyList[Channel],
                                          preferredChannels: Option[List[Channel]]): Channel = {
    val triggerPreferencesMap = preferredChannels.getOrElse(Nil).zipWithIndex.toMap
    val triggerPriority       = priority(triggerPreferencesMap) _

    availableChannels.toList
      .sortBy(costPriority)
      .sortBy(triggerPriority)
      .head
  }

  private def nonEmptyListFrom[A](list: List[A],
                                  errorMessage: String,
                                  errorCode: ErrorCode): Either[ErrorDetails, NonEmptyList[A]] = {
    NonEmptyList
      .fromList(list)
      .toRight(ErrorDetails(errorMessage, errorCode))
  }

  private def findChannelsWithTemplates(
      triggeredV4: TriggeredV4,
      templateDetails: TemplateDetails): F[Either[ErrorDetails, NonEmptyList[Channel]]] = {
    val channelsWithTemplates =
      List(templateDetails.template.email.map(_ => Email),
           templateDetails.template.sms.map(_ => SMS),
           templateDetails.template.print.map(_ => Print)).flatten

    Async[F].pure {
      nonEmptyListFrom(
        channelsWithTemplates,
        s"No valid template found for template: ${triggeredV4.metadata.templateManifest.id} version ${triggeredV4.metadata.templateManifest.version}",
        InvalidTemplate
      )
    }
  }

  private def findChannelsWithContactDetails(
      contactProfile: ContactProfile): F[Either[ErrorDetails, NonEmptyList[Channel]]] = {

    val channels = List(
      contactProfile.mobileNumber.map(_ => SMS),
      contactProfile.emailAddress.map(_ => Email),
      contactProfile.postalAddress.map(_ => Print)
    ).flatten

    Async[F].pure(nonEmptyListFrom(channels, "No contact details found", InvalidProfile))
  }
}
