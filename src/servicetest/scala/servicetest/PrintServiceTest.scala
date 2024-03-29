package servicetest

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
import com.ovoenergy.orchestration.util.TestUtil
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

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  behavior of "Aiven Print Orchestration"

  it should "orchestrate a triggered event with valid print contact details" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestratedPrint.v2,
    Kafka.aiven.feedback.v1) { (orchestratedPrintConsumer, feedbackConsumer) =>
    val triggered = TriggeredV4(
      metadata = TestUtil.metadataV3.copy(deliverTo = ContactDetails(None, None, Some(validCustomerAddress))),
      templateData = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue"))),
      deliverAt = None,
      expireAt = None,
      Some(List(Print))
    )

    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "myTemplate",
        model.Service,
        Ovo,
        triggered.metadata.templateManifest.version
      ))

    uploadTemplateToFakeS3(region, s3Endpoint)(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectOrchestratedPrintEvents(noOfEventsExpected = 1, consumer = orchestratedPrintConsumer, triggered = triggered)
    expectFeedbackEvents(noOfEventsExpected = 1,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending))
  }

  it should "raise failure for triggered event with contact details with insufficient details" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.failed.v3,
    Kafka.aiven.feedback.v1) { (failedConsumer, feedbackConsumer) =>
    val triggered = TriggeredV4(
      metadata = TestUtil.metadataV3.copy(
        deliverTo = ContactDetails(None, None, None)
      ),
      templateData = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue"))),
      deliverAt = None,
      expireAt = None,
      Some(List(Email))
    )

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

    failedConsumer
      .pollFor(noOfEventsExpected = 1)
      .foreach(failure => {
        failure.reason should include("No contact details found")
        failure.errorCode shouldBe InvalidProfile
        failure.metadata.traceToken shouldBe triggered.metadata.traceToken
      })

    expectFeedbackEvents(noOfEventsExpected = 2,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Failed))
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.failed.v3,
    Kafka.aiven.feedback.v1) { (failedConsumer, feedbackConsumer) =>
    val triggered = TestUtil.emailContactDetailsTriggered.copy(
      metadata = TestUtil.metadataV3.copy(
        deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
        templateManifest = TemplateManifest(Hash("print-only"), "0.1")
      ))

    populateTemplateSummaryTable(
      TemplateSummary(
        TemplateId(triggered.metadata.templateManifest.id),
        "myTemplate",
        model.Service,
        Ovo,
        triggered.metadata.templateManifest.version
      ))

    uploadPrintOnlyTemplateToFakeS3(region, s3Endpoint)(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)

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

  def expectOrchestratedPrintEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    consumer: KafkaConsumer[String, OrchestratedPrintV2],
                                    triggered: TriggeredV4) = {
    val orchestratedPrintEvents = consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestratedPrintEvents.map { orchestratedPrint =>
      orchestratedPrint.address shouldBe validCustomerAddress

      orchestratedPrint.customerProfile shouldBe None // We do not currently handle customer profiles for print
      orchestratedPrint.templateData shouldBe triggered.templateData

      if (shouldCheckTraceToken)
        orchestratedPrint.metadata.traceToken shouldBe triggered.metadata.traceToken
      orchestratedPrint
    }
  }
}
