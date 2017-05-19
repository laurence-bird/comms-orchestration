package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.Validated.Valid
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.model.template.processed.email.EmailTemplate
import com.ovoenergy.comms.templates.model.template.processed.sms.SMSTemplate
import com.ovoenergy.orchestration.domain.customer.{
  CommunicationPreference,
  ContactProfile,
  CustomerProfile,
  CustomerProfileEmailAddresses,
  CustomerProfileName,
  EmailAddress,
  MobilePhoneNumber
}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.util.{ArbGenerator, TestUtil}
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

class ChannelSelectorSpec extends FlatSpec with Matchers with ArbGenerator {

  val contactProfile = ContactProfile(
    Some(EmailAddress("some.email@ovoenergy.com")),
    Some(MobilePhoneNumber("123456789"))
  )

  def retrieveTemplate(template: CommTemplate[Id]) = (commManifest: CommManifest) => Valid(template)

  val emailTemplate       = generate[EmailTemplate[Id]]
  val smsTemplate         = generate[SMSTemplate[Id]]
  val serviceCommMetadata = generate[MetadataV2].copy(commManifest = CommManifest(Service, "test-comm", "1.0"))
  val triggeredBase       = TestUtil.customerTriggered

  val noChannelsTemplate  = CommTemplate(None, None)
  val emailOnlyTemplate   = CommTemplate[Id](Some(emailTemplate), None)
  val smsOnlyTemplate     = CommTemplate[Id](None, Some(smsTemplate))
  val smsAndEmailTemplate = CommTemplate[Id](Some(emailTemplate), Some(smsTemplate))

  behavior of "ChannelSelector"

  it should "Return an error if there are no templates available for the specified trigger" in {
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(noChannelsTemplate))
        .determineChannel(contactProfile, Seq(), triggeredBase)

    channelResult shouldBe
      Left(
        ErrorDetails(
          s"No valid template found for comm: ${triggeredBase.metadata.commManifest.name} version ${triggeredBase.metadata.commManifest.version}",
          InvalidTemplate
        )
      )
  }

  it should "Return the cheapest option if there are no trigger channel preferences or customer channel preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = None)
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult shouldBe Right(Email)
  }

  it should "Return the highest priority trigger channel preference if it is available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Return the second trigger channel preference if the template for the first is not available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsOnlyTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Adhere to customer preferences over trigger preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS))), triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Adhere to customer preferences over trigger preferences when they don't intersect" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(Email))), triggered)

    channelResult shouldBe Right(Email)
  }

  it should "Disregard preferences for channels not implemented in templates" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(Post))), triggered)

    channelResult shouldBe Right(SMS)
  }

  it should "Adhere to trigger preferences priority if customer profile preferences contains all the channels available" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS, Email))), triggered)

    channelResult shouldBe Right(Email)
  }

  it should "Return an error if there are available channels for a customer, but their preferences can't be met" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email)))
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(emailOnlyTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS))), triggered)

    channelResult shouldBe Left(ErrorDetails("No available channels that the customer accepts", OrchestrationError))
  }

  it should "Use the lower priority channel if the customer is missing contact details for the high priority channel" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val customerProfileAllPreferences =
      contactProfile.copy(mobileNumber = None)
    val channelResult =
      new ChannelSelectorWithTemplate(retrieveTemplate(emailOnlyTemplate))
        .determineChannel(customerProfileAllPreferences, Seq(), triggered)

    channelResult shouldBe Right(Email)
  }
}
