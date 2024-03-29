package servicetest

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model.FeedbackOptions.Pending
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
    extends BaseSpec
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with DynamoTesting
    with KafkaTesting
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

  it should "orchestrate SMS request to send immediately" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestratedSMS.v3,
    Kafka.aiven.feedback.v1) { (orchestratedSMSConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggeredV4.copy(preferredChannels = Some(List(SMS)))
    uploadTemplateToFakeS3(region, s3Endpoint)(triggered.metadata.templateManifest)

    populateTemplateSummaryTable(triggered.metadata.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectSMSOrchestrationEvents(noOfEventsExpected = 1, consumer = orchestratedSMSConsumer, triggered = triggered)
    expectFeedbackEvents(noOfEventsExpected = 1, consumer = feedbackConsumer, expectedStatuses = Set(Pending))
  }

  it should "orchestrate triggered event with sms contact details" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestratedSMS.v3,
    Kafka.aiven.feedback.v1) { (orchestratedSMSConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggeredV4.metadata.templateManifest)
    val triggered = TestUtil.smsContactDetailsTriggered
    populateTemplateSummaryTable(triggered.metadata.templateManifest)
    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectSMSOrchestrationEvents(pollTime = 120.seconds,
                                 noOfEventsExpected = 1,
                                 shouldHaveCustomerProfile = false,
                                 consumer = orchestratedSMSConsumer,
                                 triggered = triggered)
    expectFeedbackEvents(noOfEventsExpected = 1, consumer = feedbackConsumer, expectedStatuses = Set(Pending))
  }

  def expectSMSOrchestrationEvents(pollTime: FiniteDuration = 20.seconds,
                                   noOfEventsExpected: Int,
                                   shouldCheckTraceToken: Boolean = true,
                                   shouldHaveCustomerProfile: Boolean = true,
                                   consumer: KafkaConsumer[String, OrchestratedSMSV3],
                                   triggered: TriggeredV4) = {

    val orchestratedSMS = consumer.pollFor(pollTime = pollTime, noOfEventsExpected = noOfEventsExpected)

    orchestratedSMS.map { orchestratedSMS =>
      orchestratedSMS.recipientPhoneNumber shouldBe "+447985631544"

      if (shouldHaveCustomerProfile) orchestratedSMS.customerProfile shouldBe Some(CustomerProfile("John", "Wayne"))
      else orchestratedSMS.customerProfile shouldBe None

      orchestratedSMS.templateData shouldBe triggered.templateData

      if (shouldCheckTraceToken) orchestratedSMS.metadata.traceToken shouldBe triggered.metadata.traceToken
      orchestratedSMS
    }
  }
}
