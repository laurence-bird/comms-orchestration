package com.ovoenergy.orchestration.processes

import cats.Id
import cats.data.Validated.Valid
import cats.effect.IO
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import com.ovoenergy.comms.templates.model.template.processed.email.EmailTemplate
import com.ovoenergy.comms.templates.model.template.processed.sms.SMSTemplate
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.domain.{CommunicationPreference, ContactProfile, EmailAddress, MobilePhoneNumber}
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails
import com.ovoenergy.orchestration.templates.RetrieveTemplateDetails.TemplateDetails
import com.ovoenergy.orchestration.util.{ArbInstances, TestUtil}
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

class ChannelSelectorSpec extends FlatSpec with Matchers with ArbInstances {

  val contactProfile = ContactProfile(
    Some(EmailAddress("some.email@ovoenergy.com")),
    Some(MobilePhoneNumber("123456789")),
    None
  )

  def retrieveTemplate(template: CommTemplate[Id]) =
    (templateManifest: TemplateManifest) => IO.pure(Valid(TemplateDetails(template, Service)))

  val emailTemplate       = generate[EmailTemplate[Id]]
  val smsTemplate         = generate[SMSTemplate[Id]]
  val serviceCommMetadata = generate[MetadataV3].copy(templateManifest = TemplateManifest(Hash("test-comm"), "1.0"))
  val triggeredBase       = TestUtil.customerTriggeredV4

  val noChannelsTemplate  = CommTemplate(None, None, None)
  val emailOnlyTemplate   = CommTemplate[Id](Some(emailTemplate), None, None)
  val smsOnlyTemplate     = CommTemplate[Id](None, Some(smsTemplate), None)
  val smsAndEmailTemplate = CommTemplate[Id](Some(emailTemplate), Some(smsTemplate), None)

  behavior of "ChannelSelector"

  it should "Return an error if there are no templates available for the specified trigger" in {
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(noChannelsTemplate))
        .determineChannel(contactProfile, Seq(), triggeredBase)

    channelResult.unsafeRunSync() shouldBe
      Left(
        ErrorDetails(
          s"No valid template found for template: ${triggeredBase.metadata.templateManifest.id} version ${triggeredBase.metadata.templateManifest.version}",
          InvalidTemplate
        )
      )
  }

  it should "Return the cheapest option if there are no trigger channel preferences or customer channel preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = None)
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult.unsafeRunSync() shouldBe Right(Email)
  }

  it should "Return the highest priority trigger channel preference if it is available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult.unsafeRunSync() shouldBe Right(SMS)
  }

  it should "Return the second trigger channel preference if the template for the first is not available and the customer has no preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsOnlyTemplate))
        .determineChannel(contactProfile, Seq(), triggered)

    channelResult.unsafeRunSync() shouldBe Right(SMS)
  }

  it should "Adhere to customer preferences over trigger preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS))), triggered)

    channelResult.unsafeRunSync() shouldBe Right(SMS)
  }

  it should "Adhere to customer preferences over trigger preferences when they don't intersect" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(Email))), triggered)

    channelResult.unsafeRunSync() shouldBe Right(Email)
  }

  it should "Adhere to trigger preferences priority if customer profile preferences contains all the channels available" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email, SMS)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(smsAndEmailTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS, Email))), triggered)

    channelResult.unsafeRunSync() shouldBe Right(Email)
  }

  it should "Return an error if there are available channels for a customer, but their preferences can't be met" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(Email)))
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(emailOnlyTemplate))
        .determineChannel(contactProfile, Seq(CommunicationPreference(Service, Seq(SMS))), triggered)

    channelResult.unsafeRunSync() shouldBe Left(
      ErrorDetails("No available channels that the customer accepts", OrchestrationError))
  }

  it should "Use the lower priority channel if the customer is missing contact details for the high priority channel" in {
    val triggered = triggeredBase.copy(preferredChannels = Some(List(SMS, Email)))
    val customerProfileAllPreferences =
      contactProfile.copy(mobileNumber = None)
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(emailOnlyTemplate))
        .determineChannel(customerProfileAllPreferences, Seq(), triggered)

    channelResult.unsafeRunSync() shouldBe Right(Email)
  }

  it should "ignore non relevant preferences" in {
    val triggered = triggeredBase.copy(preferredChannels = None)
    val customerProfileAllPreferences =
      contactProfile.copy(mobileNumber = None)
    val channelResult =
      new ChannelSelectorWithTemplate[IO](retrieveTemplate(emailOnlyTemplate))
        .determineChannel(customerProfileAllPreferences, Seq(CommunicationPreference(Marketing, Seq(SMS))), triggered)

    channelResult.unsafeRunSync() shouldBe Right(Email)
  }
}
