package servicetest

import java.time.Instant

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.ovoenergy.comms.model.print.OrchestratedPrint
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import servicetest.helpers._
import shapeless.Coproduct

import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.orchestration.util.TestUtil.{metadataV2, traceToken}
import org.apache.kafka.clients.consumer.KafkaConsumer

class PrintServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration {

  val validCustomerAddress = CustomerAddress(line1 = "33 Notting Hill Gate",
                                             line2 = Some("office 3"),
                                             town = "Kensington",
                                             county = Some("London"),
                                             postcode = "W11 3JQ",
                                             country = Some("UK"))

  val templateData = Map("someKey" -> TemplateData(Coproduct[TemplateData.TD]("someValue")))

  val printContactDetailsTriggered = TriggeredV3(
    metadata = metadataV2.copy(deliverTo = ContactDetails(None, None, Some(validCustomerAddress))),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Print))
  )

  val invalidPrintContactDetailsTriggered = TriggeredV3(
    metadata = metadataV2.copy(
      deliverTo = ContactDetails(None, None, None)
    ),
    templateData = templateData,
    deliverAt = None,
    expireAt = None,
    Some(List(Email))
  )

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(printContactDetailsTriggered.metadata.commManifest)
  }

  override def afterAll() = {
    super.afterAll()
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  behavior of "Aiven Print Orchestration"

  it should "orchestrate a triggered event with valid print contact details" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.orchestratedPrint.v1) { (orchestrationStartedConsumer, orchestratedPrintConsumer) =>
    Kafka.aiven.triggered.v3.publishOnce(printContactDetailsTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedPrintEvents(noOfEventsExpected = 1,
                                  shouldHaveCustomerProfile = false,
                                  consumer = orchestratedPrintConsumer)
  }

  it should "raise failure for triggered event with contact details with insufficient details" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.failed.v2) { (orchestrationStartedConsumer, failedConsumer) =>
    uploadTemplateToFakeS3(region, s3Endpoint)(com.ovoenergy.orchestration.util.TestUtil.metadataV2.commManifest)
    Kafka.aiven.triggered.v3.publishOnce(invalidPrintContactDetailsTriggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No contact details found")
      failure.errorCode shouldBe InvalidProfile
      failure.metadata.traceToken shouldBe traceToken
    })
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.failed.v2) { (orchestrationStartedConsumer, failedConsumer) =>
    val commManifest = CommManifest(Service, "print-only", "0.1")
    val metadata = com.ovoenergy.orchestration.util.TestUtil.metadataV2.copy(
      deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
      commManifest = commManifest
    )
    val triggered = com.ovoenergy.orchestration.util.TestUtil.emailContactDetailsTriggered.copy(metadata = metadata)

    uploadPrintOnlyTemplateToFakeS3(region, s3Endpoint)(commManifest)

    Kafka.aiven.triggered.v3.publishOnce(triggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No available channels to deliver comm")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe com.ovoenergy.orchestration.util.TestUtil.traceToken
    })
  }

  it should "support the scheduling of triggered events for print" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.orchestratedPrint.v1) { (orchestrationStartedConsumer, orchestratedPrintConsumer) =>
    val scheduledPrintevent = printContactDetailsTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(5)))
    Kafka.aiven.triggered.v3.publishOnce(scheduledPrintevent)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedPrintEvents(noOfEventsExpected = 1,
                                  shouldHaveCustomerProfile = false,
                                  consumer = orchestratedPrintConsumer)
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV2]]) = {

    val orchestrationStartedEvents = consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe com.ovoenergy.orchestration.util.TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedPrintEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    useMagicByte: Boolean = true,
                                    shouldHaveCustomerProfile: Boolean = true,
                                    consumer: KafkaConsumer[String, Option[OrchestratedPrint]]) = {
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
