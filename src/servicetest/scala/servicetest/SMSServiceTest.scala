package servicetest

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import servicetest.helpers._

import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import org.apache.kafka.clients.consumer.KafkaConsumer

class SMSServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration {

  val mockServerClient = new MockServerClient("localhost", 1080)

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val region     = config.getString("aws.region")
  val s3Endpoint = "http://localhost:4569"

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  behavior of "SMS Orchestration"

  it should "orchestrate SMS request to send immediately" in withThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.orchestratedSMS.v2) { (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
    val triggerSMS = TestUtil.customerTriggered.copy(preferredChannels = Some(List(SMS)))
    Kafka.aiven.triggered.v3.publishOnce(triggerSMS)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectSMSOrchestrationEvents(noOfEventsExpected = 1, consumer = orchestratedSMSConsumer)
  }

  it should "orchestrate multiple SMS" in withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2,
                                                                   Kafka.aiven.orchestratedSMS.v2) {
    (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
      createOKCustomerProfileResponse(mockServerClient)
      (1 to 10).foreach(counter => {
        val triggered = TestUtil.customerTriggered.copy(metadata =
                                                          TestUtil.metadataV2.copy(traceToken = counter.toString),
                                                        preferredChannels = Some(List(SMS)))
        Kafka.aiven.triggered.v3.publishOnce(triggered, 10.seconds)
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
    Kafka.aiven.orchestrationStarted.v2,
    Kafka.aiven.orchestratedSMS.v2) { (orchestrationStartedConsumer, orchestratedSMSConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
    val triggerSMS = TestUtil.smsContactDetailsTriggered
    Kafka.aiven.triggered.v3.publishOnce(triggerSMS)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectSMSOrchestrationEvents(pollTime = 120.seconds,
                                 noOfEventsExpected = 1,
                                 shouldHaveCustomerProfile = false,
                                 consumer = orchestratedSMSConsumer)
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25.seconds,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV2]]) = {
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
                                   consumer: KafkaConsumer[String, Option[OrchestratedSMSV2]]) = {

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