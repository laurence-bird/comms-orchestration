package servicetest

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.comms.orchestration.util.TestUtil._
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import servicetest.helpers._

import scala.concurrent.duration._
import monocle.macros.syntax.lens._
import com.ovoenergy.comms.templates.util.Hash
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.comms.templates.model.Brand
import com.ovoenergy.comms.templates.model.template.metadata.{TemplateId, TemplateSummary}

class EmailServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration {

  val s3Endpoint = "http://localhost:4569"
  val s3Region   = "eu-west-1"

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(s3Region, s3Endpoint)
    uploadTemplateToFakeS3(s3Region, s3Endpoint)(customerTriggeredV4.metadata.templateManifest)
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)

  behavior of "Email Orchestration"

  it should "orchestrate emails request to send immediately" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedEmail.v4) { (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
    val triggered = customerTriggeredV4
    createOKCustomerProfileResponse(mockServerClient)
    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "blah blah blah",
        Service,
        Brand.Ovo,
        triggered.metadata.templateManifest.version
      )
    )
    Kafka.aiven.triggered.v4.publishOnce(customerTriggeredV4)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1,
                                     consumer = orchestrationStartedConsumer,
                                     pollTime = 40.seconds)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer, pollTime = 40.seconds)
  }

  it should "orchestrate multiple emails" in withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v3,
                                                                      Kafka.aiven.orchestratedEmail.v4) {
    (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
      createOKCustomerProfileResponse(mockServerClient)
      val triggered = customerTriggeredV4

      populateTemplateSummaryTable(
        TemplateSummary(
          TemplateId(triggered.metadata.templateManifest.id),
          "blah blah blah",
          Service,
          Brand.Ovo,
          triggered.metadata.templateManifest.version
        )
      )

      (1 to 10).foreach(counter => {
        val newTriggered =
          triggered.copy(metadata = metadataV3.copy(traceToken = counter.toString))
        Kafka.aiven.triggered.v4.publishOnce(newTriggered, timeout = 10.seconds)
      })

      val deadline = 15.seconds
      val orchestrationStartedEvents =
        expectOrchestrationStartedEvents(noOfEventsExpected = 10,
                                         shouldCheckTraceToken = false,
                                         pollTime = (25000 * 10).millisecond,
                                         consumer = orchestrationStartedConsumer)
      orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
      val orchestratedEmails =
        expectOrchestratedEmailEvents(pollTime = deadline,
                                      noOfEventsExpected = 10,
                                      shouldCheckTraceToken = false,
                                      useMagicByte = false,
                                      consumer = orchestratedEmailConsumer)
      orchestratedEmails.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  it should "raise failure for customers with insufficient details to orchestrate emails for" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.failed.v3) { (orchestrationStartedConsumer, failedConsumer) =>
    failedConsumer.checkNoMessages(5.second)
    uploadTemplateToFakeS3(s3Region, s3Endpoint)(metadataV3.templateManifest)
    createInvalidCustomerProfileResponse(mockServerClient)

    val triggered = customerTriggeredV4
    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "blah blah blah",
        Service,
        Brand.Ovo,
        triggered.metadata.templateManifest.version
      )
    )

    Kafka.aiven.triggered.v4.publishOnce(triggered)

    withClue("No orchestration started events")(
      expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer))
    val failures = withClue("No failed events")(failedConsumer.pollFor(pollTime = 120.seconds, noOfEventsExpected = 1))
    failures.foreach(failure => {
      failure.reason should include("No contact details found")
      failure.errorCode shouldBe InvalidProfile
      failure.metadata.traceToken shouldBe traceToken
    })
  }

  it should "retry if the profile service returns an error response" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedEmail.v4) { (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
    createFlakyCustomerProfileResponse(mockServerClient)

    val triggered = customerTriggeredV4
    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "blah blah blah",
        Service,
        Brand.Ovo,
        triggered.metadata.templateManifest.version
      )
    )

    Kafka.aiven.triggered.v4.publishOnce(triggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)
  }

  it should "orchestrate triggered event with email contact details" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedEmail.v4) { (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
    Kafka.aiven.triggered.v4.publishOnce(emailContactDetailsTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1,
                                  shouldHaveCustomerProfile = false,
                                  consumer = orchestratedEmailConsumer)
  }

  it should "raise failure for triggered event with contact details with insufficient details" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.failed.v3) { (orchestrationStartedConsumer, failedConsumer) =>
    uploadTemplateToFakeS3(s3Region, s3Endpoint)(metadataV3.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(invalidContactDetailsTriggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No contact details found")
      failure.errorCode shouldBe InvalidProfile
      failure.metadata.traceToken shouldBe traceToken
    })
  }

  it should "raise failure for triggered event with a list of fields having empty string as their value" in withThrowawayConsumerFor(
    Kafka.aiven.failed.v3) { failedConsumer =>
    val td = Map(
      "firstName" -> TemplateData.fromString("Joe"),
      "lastName"  -> TemplateData.fromString(""),
      "roles" -> TemplateData.fromSeq(
        List[TemplateData](TemplateData.fromString("admin"), TemplateData.fromString("")))
    )

    val emptyTraceToken = TriggeredV4(
      metadata = metadataV3.lens(_.traceToken).modify(_ => ""),
      templateData = Map("person" -> TemplateData.fromMap(td)),
      deliverAt = None,
      expireAt = None,
      Some(List(Email))
    )

    uploadTemplateToFakeS3(s3Region, s3Endpoint)(metadataV3.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(emptyTraceToken)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include(
        "The following fields contain empty string: traceToken, templateData.person.lastName, templateData.person.roles")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe ""
    })
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.failed.v3) { (orchestrationStartedConsumer, failedConsumer) =>
    val templateManifest = TemplateManifest(Hash("sms-only"), "0.1")
    val metadata = metadataV3.copy(
      deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
      templateManifest = templateManifest
    )
    val triggered = emailContactDetailsTriggered.copy(metadata = metadata)

    uploadSMSOnlyTemplateToFakeS3(s3Region, s3Endpoint)(templateManifest)
    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "blah blah blah",
        Service,
        Brand.Ovo,
        triggered.metadata.templateManifest.version
      )
    )

    Kafka.aiven.triggered.v4.publishOnce(triggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No available channels to deliver comm")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe traceToken
    })
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV3]]) = {

    val orchestrationStartedEvents =
      consumer.pollFor(noOfEventsExpected = noOfEventsExpected, pollTime = pollTime)

    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    useMagicByte: Boolean = true,
                                    shouldHaveCustomerProfile: Boolean = true,
                                    consumer: KafkaConsumer[String, Option[OrchestratedEmailV4]]) = {
    val orchestratedEmails =
      consumer.pollFor(noOfEventsExpected = noOfEventsExpected, pollTime = pollTime)

    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"

      if (shouldHaveCustomerProfile) orchestratedEmail.customerProfile shouldBe Some(CustomerProfile("John", "Wayne"))
      else orchestratedEmail.customerProfile shouldBe None

      orchestratedEmail.templateData shouldBe templateData

      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe traceToken
      orchestratedEmail
    }
  }
}
