package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.Validated.Valid
import com.ovoenergy.comms.model.Channel._
import com.ovoenergy.comms.model.CommType.Service
import com.ovoenergy.comms.model.ErrorCode.InvalidTemplate
import com.ovoenergy.comms.model.{CommManifest, SMSStatus, Triggered, TriggeredV2}
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.model.template.processed.email.EmailTemplate
import com.ovoenergy.comms.templates.model.template.processed.sms.SMSTemplate
import com.ovoenergy.orchestration.domain.customer.{
  CommunicationPreference,
  CustomerProfile,
  CustomerProfileEmailAddresses,
  CustomerProfileName
}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

class ChannelSelectorSpec extends FlatSpec with Matchers with ArbGenerator {

  val customerProfileNoPreferences = CustomerProfile(
    CustomerProfileName(Some("Mr"), "John", "Smith", None),
    CustomerProfileEmailAddresses(Some("some.email@ovoenergy.com"), None),
    Some("some.email@ovoenergy.com"),
    Some("123456789"),
    Seq.empty
  )

  def retrieveTemplate(template: CommTemplate[Id]) = (commManifest: CommManifest) => Valid(template)

  val emailTemplate = generate[EmailTemplate[Id]]
  val smsTemplate   = generate[SMSTemplate[Id]]
  val triggeredBase = generate[TriggeredV2]

  val noChannelsTemplate  = CommTemplate(None, None)
  val emailOnlyTemplate   = CommTemplate[Id](Some(emailTemplate), None)
  val smsOnlyTemplate     = CommTemplate[Id](None, Some(smsTemplate))
  val smsAndEmailTemplate = CommTemplate[Id](Some(emailTemplate), Some(smsTemplate))

  behavior of "ChannelSelector"

  it should "Return an error if there are no channels available for the specified trigger" in {
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(noChannelsTemplate))(customerProfileNoPreferences,
                                                                             triggeredBase)

    channelResult shouldBe
      Left(
        ErrorDetails(
          s"No Channel found for comm: ${triggeredBase.metadata.commManifest.name} version ${triggeredBase.metadata.commManifest.version}",
          InvalidTemplate)
      )
  }

  it should "Return email if there are no trigger channel preferences or customer channel preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = None)
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(smsAndEmailTemplate))(customerProfileNoPreferences, triggered)

    channelResult shouldBe Right(Email)
  }

  it should "Return the highest priority trigger channel preference if it is available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(smsAndEmailTemplate))(customerProfileNoPreferences, triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Return the second trigger channel preference if the first is not available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(smsOnlyTemplate))(customerProfileNoPreferences, triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Adhere to customer preferences over trigger preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val customerProfileSMSPreference =
      customerProfileNoPreferences.copy(communicationPreferences = Seq(CommunicationPreference(Service, Seq(SMS))))
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(smsOnlyTemplate))(customerProfileNoPreferences, triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Adhere to trigger preferences priority if customer profile preferences contains all the channels available" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val customerProfileSMSPreference =
      customerProfileNoPreferences.copy(
        communicationPreferences = Seq(CommunicationPreference(Service, Seq(SMS, Email))))
    val channelResult =
      ChannelSelector.determineChannel(retrieveTemplate(smsAndEmailTemplate))(customerProfileNoPreferences, triggered)

    channelResult shouldBe Right(Email)
  }

}
