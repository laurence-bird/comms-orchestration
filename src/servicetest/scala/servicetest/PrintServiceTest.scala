package servicetest

import java.time.Instant

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.print.OrchestratedPrintV2
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import servicetest.helpers._
import shapeless.Coproduct

import scala.concurrent.duration._
import com.ovoenergy.comms.templates.model.Brand.Ovo
import com.ovoenergy.comms.templates.model.template.metadata.{TemplateId, TemplateSummary}
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.util.TestUtil.{metadataV3, traceToken}
import org.apache.kafka.clients.consumer.KafkaConsumer

class PrintServiceTest
    extends BaseSpec
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with DynamoTesting
    with KafkaTesting
    with FakeS3Configuration {

  val validCustomerAddress = CustomerAddress(line1 = "33 Notting Hill Gate",
                                             line2 = Some("office 3"),
                                             town = "Kensington",
                                             county = Some("London"),
                                             postcode = "W11 3JQ",
                                             country = Some("UK"))

  val templateData = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue")))

  val printContactDetailsTriggered = TriggeredV4(
    metadata = metadataV3.copy(deliverTo = ContactDetails(None, None, Some(validCustomerAddress))),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Print))
  )

  val invalidPrintContactDetailsTriggered = TriggeredV4(
    metadata = metadataV3.copy(
      deliverTo = ContactDetails(None, None, None)
    ),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  val allTestTriggers = List(printContactDetailsTriggered, invalidPrintContactDetailsTriggered)

  override def beforeAll() = {
    super.beforeAll()
    allTestTriggers.foreach { t =>
      populateTemplateSummaryTable(
        TemplateSummary(
          TemplateId(t.metadata.templateManifest.id),
          "myTemplate",
          model.Service,
          Ovo,
          t.metadata.templateManifest.version
        ))

    }
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(printContactDetailsTriggered.metadata.templateManifest)
  }

  override def afterAll() = {
    super.afterAll()
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  behavior of "Aiven Print Orchestration"

  it should "orchestrate a triggered event with valid print contact details" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedPrint.v2,
    Kafka.aiven.feedback.v1) { (orchestrationStartedConsumer, orchestratedPrintConsumer, feedbackConsumer) =>
    Kafka.aiven.triggered.v4.publishOnce(printContactDetailsTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedPrintEvents(noOfEventsExpected = 1, consumer = orchestratedPrintConsumer)
    expectFeedbackEvents(noOfEventsExpected = 1,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending))
  }

  it should "raise failure for triggered event with contact details with insufficient details" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.failed.v3,
    Kafka.aiven.feedback.v1) { (orchestrationStartedConsumer, failedConsumer, feedbackConsumer) =>
    val triggered = invalidPrintContactDetailsTriggered
    uploadTemplateToFakeS3(region, s3Endpoint)(triggered.metadata.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(triggered)

    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "myTemplate",
        model.Service,
        Ovo,
        triggered.metadata.templateManifest.version
      ))

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    failedConsumer
      .pollFor(noOfEventsExpected = 1)
      .foreach(failure => {
        failure.reason should include("No contact details found")
        failure.errorCode shouldBe InvalidProfile
        failure.metadata.traceToken shouldBe traceToken
      })

    expectFeedbackEvents(noOfEventsExpected = 2,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Failed))
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.failed.v3,
    Kafka.aiven.feedback.v1) { (orchestrationStartedConsumer, failedConsumer, feedbackConsumer) =>
    val templateManifest = TemplateManifest(Hash("print-only"), "0.1")
    val metadata = com.ovoenergy.orchestration.util.TestUtil.metadataV3.copy(
      deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
      templateManifest = templateManifest
    )
    val triggered = com.ovoenergy.orchestration.util.TestUtil.emailContactDetailsTriggered.copy(metadata = metadata)

    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "myTemplate",
        model.Service,
        Ovo,
        triggered.metadata.templateManifest.version
      ))

    uploadPrintOnlyTemplateToFakeS3(region, s3Endpoint)(templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No available channels to deliver comm")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe com.ovoenergy.orchestration.util.TestUtil.traceToken
    })

    expectFeedbackEvents(noOfEventsExpected = 2,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Failed))
  }

  it should "support the scheduling of triggered events for print" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedPrint.v2,
    Kafka.aiven.feedback.v1) { (orchestrationStartedConsumer, orchestratedPrintConsumer, feedbackConsumer) =>
    val scheduledPrintevent = printContactDetailsTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(5)))

    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(scheduledPrintevent.metadata.templateManifest.id),
        "myTemplate",
        model.Service,
        Ovo,
        scheduledPrintevent.metadata.templateManifest.version
      ))

    Kafka.aiven.triggered.v4.publishOnce(scheduledPrintevent)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedPrintEvents(noOfEventsExpected = 1, consumer = orchestratedPrintConsumer)
    expectFeedbackEvents(noOfEventsExpected = 2,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Scheduled))
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV3]]) = {

    val orchestrationStartedEvents = consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe com.ovoenergy.orchestration.util.TestUtil.traceToken
    }
    orchestrationStartedEvents
  }
  def expectOrchestratedPrintEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    consumer: KafkaConsumer[String, Option[OrchestratedPrintV2]]) = {
    val orchestratedPrintEvents = consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestratedPrintEvents.map { orchestratedPrint =>
      orchestratedPrint.address shouldBe validCustomerAddress

      orchestratedPrint.customerProfile shouldBe None // We do not currently handle customer profiles for print
      orchestratedPrint.templateData shouldBe templateData

      if (shouldCheckTraceToken)
        orchestratedPrint.metadata.traceToken shouldBe printContactDetailsTriggered.metadata.traceToken
      orchestratedPrint
    }
  }
}
