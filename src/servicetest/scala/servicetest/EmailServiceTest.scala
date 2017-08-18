package servicetest

import com.ovoenergy.comms.helpers.Kafka
import servicetest.helpers._
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.{OrchestratedEmailV2, OrchestratedEmailV3}
import com.ovoenergy.orchestration.util.TestUtil
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest._
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import org.apache.kafka.clients.consumer.KafkaConsumer

class EmailServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration
    with KafkaTesting {

  override def beforeAll() = {
    super.beforeAll()
    allKafkaConsumers.foreach(_.poll(100))
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  override def afterAll() = {
    allKafkaConsumers.foreach(_.close)
    super.afterAll()
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  lazy val orchestrationStartedConsumer = Kafka.aiven.orchestrationStarted.v2.consumer()
  lazy val orchestratedEmailConsumer    = Kafka.aiven.orchestratedEmail.v3.consumer()
  lazy val failedConsumer               = Kafka.aiven.failed.v2.consumer()

  private def allKafkaConsumers = Seq(
    orchestrationStartedConsumer,
    orchestratedEmailConsumer,
    failedConsumer
  )

  behavior of "Email Orchestration"

  // Upload valid template to S3 with both email and SMS templates available

  it should "orchestrate emails request to send immediately" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        createOKCustomerProfileResponse(mockServerClient)
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)
        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)
    }
  }

  it should "orchestrate legacy emails request to send immediately" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        createOKCustomerProfileResponse(mockServerClient)
        Kafka.legacy.triggered.v2.publishOnce(TestUtil.legacyTriggered)

        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)
    }
  }

  it should "generate unique internalTraceTokens" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        createOKCustomerProfileResponse(mockServerClient)
        Kafka.legacy.triggered.v3.publishOnce(TestUtil.customerTriggered)
        Kafka.legacy.triggered.v3.publishOnce(TestUtil.customerTriggered)

        expectOrchestrationStartedEvents(noOfEventsExpected = 2, consumer = orchestrationStartedConsumer)
        val events = expectOrchestratedEmailEvents(noOfEventsExpected = 2, consumer = orchestratedEmailConsumer)
        events match {
          case head :: tail =>
            head.internalMetadata.internalTraceToken should not equal tail.head.internalMetadata.internalTraceToken
        }
    }
  }

  it should "orchestrate multiple emails" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        createOKCustomerProfileResponse(mockServerClient)

        (1 to 10).foreach(counter => {
          val triggered =
            TestUtil.customerTriggered.copy(metadata = TestUtil.metadataV2.copy(traceToken = counter.toString))
          Kafka.legacy.triggered.v3.publishOnce(triggered, 10.seconds)
        })

        val deadline = 15.seconds
        val orchestrationStartedEvents =
          expectOrchestrationStartedEvents(noOfEventsExpected = 10,
                                           shouldCheckTraceToken = false,
                                           consumer = orchestrationStartedConsumer)
        orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
        val orchestratedEmails =
          expectOrchestratedEmailEvents(pollTime = deadline,
                                        noOfEventsExpected = 10,
                                        shouldCheckTraceToken = false,
                                        consumer = orchestratedEmailConsumer)
        orchestratedEmails.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    }
  }
  it should "raise failure for customers with insufficient details to orchestrate emails for" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.failed.v2) {
      (orchestrationStartedConsumer, failedConsumer) =>
        uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadata.commManifest)
        createInvalidCustomerProfileResponse(mockServerClient)
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)
        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

        val failures = failedConsumer.pollFor(5.seconds)
        failures.size shouldBe 1
        failures.foreach(failure => {
          failure.reason should include("No contact details found")
          failure.errorCode shouldBe InvalidProfile
          failure.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  it should "raise failure when customer profiler fails" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.failed.v2) {
      (orchestrationStartedConsumer, failedConsumer) =>
        createBadCustomerProfileResponse(mockServerClient)
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)

        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        val failures = failedConsumer.pollFor(5.seconds)
        failures.size shouldBe 1
        failures.foreach(failure => {
          failure.reason should include("Error response (500) from profile service: Some error")
          failure.metadata.traceToken shouldBe TestUtil.traceToken
          failure.errorCode shouldBe ProfileRetrievalFailed
        })
    }
  }

  it should "retry if the profile service returns an error response" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        createFlakyCustomerProfileResponse(mockServerClient)
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)

        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)

    }
  }
  it should "orchestrate triggered event with email contact details" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.emailContactDetailsTriggered)

        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        expectOrchestratedEmailEvents(noOfEventsExpected = 1,
                                      shouldHaveCustomerProfile = false,
                                      consumer = orchestratedEmailConsumer)
    }
  }

  it should "raise failure for triggered event with contact details with insufficient details" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.failed.v2) {
      (orchestrationStartedConsumer, failedConsumer) =>
        uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadata.commManifest)
        Kafka.aiven.triggered.v3.publishOnce(TestUtil.invalidContactDetailsTriggered)
        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
        failures.foreach(failure => {
          failure.reason should include("No contact details found")
          failure.errorCode shouldBe InvalidProfile
          failure.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in {
    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.failed.v2) {
      (orchestrationStartedConsumer, failedConsumer) =>
        val commManifest = CommManifest(Service, "sms-only", "0.1")
        val metadata = TestUtil.metadataV2.copy(
          deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
          commManifest = commManifest
        )
        val triggered = TestUtil.emailContactDetailsTriggered.copy(metadata = metadata)

        uploadSMSOnlyTemplateToFakeS3(region, s3Endpoint)(commManifest)
        Kafka.aiven.triggered.v3.publishOnce(triggered)
        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)

        val failures = failedConsumer.pollFor(5.seconds)
        failures.size shouldBe 1
        failures.foreach(failure => {
          failure.reason should include("No available channels to deliver comm")
          failure.errorCode shouldBe OrchestrationError
          failure.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  it should "orchestrate legacy TriggeredV2 comms with the hacky deserialiser" in {
    case class HackyTriggeredV2(metadata: Metadata, templateData: Map[String, TemplateData])

    val hackyLegacyTriggered = HackyTriggeredV2(
      metadata = TestUtil.metadata,
      templateData = TestUtil.templateData
    )

    withThrowawayConsumerFor(Kafka.aiven.orchestrationStarted.v2, Kafka.aiven.orchestratedEmail.v3) {
      (orchestrationStartedConsumer, orchestratedEmailConsumer) =>
        val triggered: HackyTriggeredV2 = hackyLegacyTriggered
        legacyPublishOnce[HackyTriggeredV2](Kafka.legacy.triggered.v2.name,
                                            Kafka.legacy.kafkaConfig.hosts,
                                            triggered,
                                            5.seconds)

        expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
        expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)
    }
  }

  def expectOrchestrationStartedEvents(consumer: KafkaConsumer[String, Option[OrchestrationStartedV2]],
                                       pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    val orchestrationStartedEvents = consumer.pollFor(pollTime, noOfEventsExpected)
    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedEmailEvents(consumer: KafkaConsumer[String, Option[OrchestratedEmailV3]],
                                    pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    shouldHaveCustomerProfile: Boolean = true) = {
    val orchestratedEmails = consumer.pollFor(pollTime, noOfEventsExpected)
    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"

      if (shouldHaveCustomerProfile) orchestratedEmail.customerProfile shouldBe Some(CustomerProfile("John", "Wayne"))
      else orchestratedEmail.customerProfile shouldBe None

      orchestratedEmail.templateData shouldBe TestUtil.templateData

      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }
}
