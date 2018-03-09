package servicetest

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory}
import org.mockserver.client.server.MockServerClient
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import servicetest.helpers._
import com.ovoenergy.comms.serialisation.Codecs._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import monocle.macros.syntax.lens._

class AivenEmailServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration {

  override def beforeAll() = {
    super.beforeAll()
    allKafkaConsumers().foreach(_.poll(5))
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  override def afterAll() = {
    allKafkaConsumers().foreach(_.close())
    super.afterAll()
  }

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  lazy val failedConsumer               = Kafka.aiven.failed.v2.consumer()
  lazy val orchestratedEmailConsumer    = Kafka.aiven.orchestratedEmail.v3.consumer()
  lazy val orchestrationStartedConsumer = Kafka.aiven.orchestrationStarted.v2.consumer()

  private def allKafkaConsumers() = Seq(failedConsumer, orchestratedEmailConsumer, orchestrationStartedConsumer)

  behavior of "Email Orchestration"

  behavior of "Aiven Email Orchestration"

  it should "orchestrate emails request to send immediately" in {
    createOKCustomerProfileResponse(mockServerClient)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "orchestrate multiple emails" in {
    createOKCustomerProfileResponse(mockServerClient)

    (1 to 10).foreach(counter => {
      val triggered =
        TestUtil.customerTriggered.copy(metadata = TestUtil.metadataV2.copy(traceToken = counter.toString))
      Kafka.aiven.triggered.v3.publishOnce(triggered, timeout = 10.seconds)
    })

    val deadline = 15.seconds
    val orchestrationStartedEvents =
      expectOrchestrationStartedEvents(noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    val orchestratedEmails =
      expectOrchestratedEmailEvents(pollTime = deadline,
                                    noOfEventsExpected = 10,
                                    shouldCheckTraceToken = false,
                                    useMagicByte = false)
    orchestratedEmails.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  it should "generate unique internalTraceTokens" in {
    createOKCustomerProfileResponse(mockServerClient)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 2)
    val events = expectOrchestratedEmailEvents(noOfEventsExpected = 2)
    events match {
      case head :: tail =>
        head.internalMetadata.internalTraceToken should not equal tail.head.internalMetadata.internalTraceToken
    }
  }

  it should "raise failure for customers with insufficient details to orchestrate emails for" in {
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadataV2.commManifest)
    createInvalidCustomerProfileResponse(mockServerClient)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No contact details found")
      failure.errorCode shouldBe InvalidProfile
      failure.metadata.traceToken shouldBe TestUtil.traceToken
    })
  }

  it should "raise failure when customer profiler fails" in {
    createBadCustomerProfileResponse(mockServerClient)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("Error response (500) from profile service: Some error")
      failure.metadata.traceToken shouldBe TestUtil.traceToken
      failure.errorCode shouldBe ProfileRetrievalFailed
    })
  }

  it should "retry if the profile service returns an error response" in {
    createFlakyCustomerProfileResponse(mockServerClient)

    Kafka.aiven.triggered.v3.publishOnce(TestUtil.customerTriggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "orchestrate triggered event with email contact details" in {
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.emailContactDetailsTriggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1, shouldHaveCustomerProfile = false)
  }

  it should "raise failure for triggered event with contact details with insufficient details" in {
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadataV2.commManifest)
    Kafka.aiven.triggered.v3.publishOnce(TestUtil.invalidContactDetailsTriggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No contact details found")
      failure.errorCode shouldBe InvalidProfile
      failure.metadata.traceToken shouldBe TestUtil.traceToken
    })
  }

  it should "raise failure for triggered event with a list of fields having empty string as their value" in {

    val td = Map(
      "firstName" -> TemplateData.fromString("Joe"),
      "lastName"  -> TemplateData.fromString(""),
      "roles" -> TemplateData.fromSeq(
        List[TemplateData](TemplateData.fromString("admin"), TemplateData.fromString("")))
    )

    val emptyTraceToken = TriggeredV3(
      metadata = TestUtil.metadataV2.lens(_.traceToken).modify(_ => ""),
      templateData = Map("person" -> TemplateData.fromMap(td)),
      deliverAt = None,
      expireAt = None,
      Some(List(Email))
    )

    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadataV2.commManifest)
    Kafka.aiven.triggered.v3.publishOnce(emptyTraceToken)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include(
        "The following fields contain empty string: traceToken, templateData.person.lastName, templateData.person.roles")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe ""
    })
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" in {
    val commManifest = CommManifest(Service, "sms-only", "0.1")
    val metadata = TestUtil.metadataV2.copy(
      deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
      commManifest = commManifest
    )
    val triggered = TestUtil.emailContactDetailsTriggered.copy(metadata = metadata)

    uploadSMSOnlyTemplateToFakeS3(region, s3Endpoint)(commManifest)

    Kafka.aiven.triggered.v3.publishOnce(triggered)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)

    val failures = failedConsumer.pollFor(noOfEventsExpected = 1)
    failures.foreach(failure => {
      failure.reason should include("No available channels to deliver comm")
      failure.errorCode shouldBe OrchestrationError
      failure.metadata.traceToken shouldBe TestUtil.traceToken
    })
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {

    val orchestrationStartedEvents = orchestrationStartedConsumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    useMagicByte: Boolean = true,
                                    shouldHaveCustomerProfile: Boolean = true) = {
    val orchestratedEmails = orchestratedEmailConsumer.pollFor(noOfEventsExpected = noOfEventsExpected)

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
