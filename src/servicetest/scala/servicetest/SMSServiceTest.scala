package servicetest

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.sms.{OrchestratedSMSV2, OrchestratedSMSV3}
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import servicetest.helpers._

import scala.concurrent.duration._
import org.apache.kafka.clients.consumer.KafkaConsumer

class SMSServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with DynamoTesting
    with FakeS3Configuration {

  val mockServerClient = new MockServerClient("localhost", 1080)

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val region     = config.getString("aws.region")
  val s3Endpoint = "http://localhost:4569"

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggeredV4.metadata.templateManifest)
  }

  behavior of "SMS Orchestration"

  it should "orchestrate SMS request to send immediately" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedSMS.v3) { (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggeredV4.metadata.templateManifest)
    val triggerSMS = TestUtil.customerTriggeredV4.copy(preferredChannels = Some(List(SMS)))
    populateTemplateSummaryTable(triggerSMS.metadata.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(triggerSMS)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectSMSOrchestrationEvents(noOfEventsExpected = 1, consumer = orchestratedSMSConsumer)
  }

  it should "orchestrate multiple SMS" in withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v3,
                                                                   Kafka.aiven.orchestratedSMS.v3) {
    (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
      createOKCustomerProfileResponse(mockServerClient)
      val triggerSMS = TestUtil.customerTriggeredV4.copy(preferredChannels = Some(List(SMS)))
      populateTemplateSummaryTable(triggerSMS.metadata.templateManifest)
      (1 to 10).foreach(counter => {
        val triggered = triggerSMS.copy(metadata = TestUtil.metadataV3.copy(traceToken = counter.toString))
        Kafka.aiven.triggered.v4.publishOnce(triggered, 10.seconds)
      })

      val deadline = 15.seconds
      val orchestrationStartedEvents =
        expectOrchestrationStartedEvents(
          pollTime = 120.seconds,
          noOfEventsExpected = 10,
          shouldCheckTraceToken = false,
          consumer = orchestrationStartedConsumer
        )

      orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

      val orchestratedSMSes =
        expectSMSOrchestrationEvents(
          pollTime = deadline,
          noOfEventsExpected = 10,
          shouldCheckTraceToken = false,
          consumer = orchestratedSMSConsumer
        )
      orchestratedSMSes.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  it should "orchestrate triggered event with sms contact details" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedSMS.v3) { (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggeredV4.metadata.templateManifest)
    val triggerSMS = TestUtil.smsContactDetailsTriggered
    populateTemplateSummaryTable(triggerSMS.metadata.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(triggerSMS)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectSMSOrchestrationEvents(pollTime = 120.seconds,
                                 noOfEventsExpected = 1,
                                 shouldHaveCustomerProfile = false,
                                 consumer = orchestratedSMSConsumer)
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25.seconds,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV3]]) = {
    val orchestrationStartedEvents =
      consumer.pollFor(pollTime = pollTime, noOfEventsExpected = noOfEventsExpected)

    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectSMSOrchestrationEvents(pollTime: FiniteDuration = 20.seconds,
                                   noOfEventsExpected: Int,
                                   shouldCheckTraceToken: Boolean = true,
                                   shouldHaveCustomerProfile: Boolean = true,
                                   consumer: KafkaConsumer[String, Option[OrchestratedSMSV3]]) = {

    val orchestratedSMS = consumer.pollFor(pollTime = pollTime, noOfEventsExpected = noOfEventsExpected)

    orchestratedSMS.map { orchestratedSMS =>
      orchestratedSMS.recipientPhoneNumber shouldBe "+447985631544"

      if (shouldHaveCustomerProfile) orchestratedSMS.customerProfile shouldBe Some(CustomerProfile("John", "Wayne"))
      else orchestratedSMS.customerProfile shouldBe None

      orchestratedSMS.templateData shouldBe TestUtil.templateData

      if (shouldCheckTraceToken) orchestratedSMS.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedSMS
    }
  }
}
