package servicetest

import java.time.Instant
import java.util.UUID

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV4
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.mockserver.client.server.MockServerClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import servicetest.helpers._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SchedulingServiceTest
    extends BaseSpec
    with MockProfileResponses
    with KafkaTesting
    with FakeS3Configuration
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit val config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  val uploadTemplate: (TemplateManifest) => Unit = uploadTemplateToFakeS3(region, s3Endpoint)

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggeredV4.metadata.templateManifest)
  }

  behavior of "Aiven comms scheduling"

  it should "deschedule comms and generate cancelled events - cancellationRequestV3" in withMultipleThrowawayConsumerFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedEmail.v4,
    Kafka.aiven.cancelled.v3,
    Kafka.aiven.feedback.v1,
  ) { (orchestrationStartedConsumer, orchestratedEmailConsumer, cancelledConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggeredV4.copy(deliverAt = Some(Instant.now().plusSeconds(60)))

    populateTemplateSummaryTable(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)
    Thread.sleep(5000) // give it time to write the events to the DB

    val genericMetadata = GenericMetadataV3(createdAt = Instant.now(),
                                            eventId = UUID.randomUUID().toString,
                                            traceToken = UUID.randomUUID().toString,
                                            source = "service test",
                                            canary = false,
                                            commId = "1234")
    val cancellationRequested: CancellationRequestedV3 =
      CancellationRequestedV3(genericMetadata, triggered.metadata.templateManifest.id, TestUtil.customerId)
    Kafka.aiven.cancellationRequested.v3.publishOnce(cancellationRequested)

    //Check that orchestration wasn't started
    orchestrationStartedConsumer.checkNoMessages()
    orchestratedEmailConsumer.checkNoMessages()

    val cancelledComms: Seq[CancelledV3] = cancelledConsumer.pollFor(noOfEventsExpected = 1)

    val cancelledComm = cancelledComms.head

    cancelledComm.metadata.traceToken shouldBe triggered.metadata.traceToken
    cancelledComm.cancellationRequested shouldBe cancellationRequested
    expectFeedbackEvents(noOfEventsExpected = 3, consumer = feedbackConsumer, expectedStatuses = Set(FeedbackOptions.Scheduled, FeedbackOptions.Cancelled))
  }

  it should "generate a cancellationFailed event if unable to deschedule a comm" in withMultipleThrowawayConsumerFor(
    Kafka.aiven.failedCancellation.v3,
    Kafka.aiven.orchestratedEmail.v4,
    Kafka.aiven.cancelled.v3,
    Kafka.aiven.feedback.v1) { (failedCancellationConsumer, orchestratedEmailConsumer, cancelledConsumer, feedbackConsumer) =>

    val triggered  = TestUtil.customerTriggeredV4.copy(deliverAt = Some(Instant.now().plusSeconds(60)))
    val templateId = triggered.metadata.templateManifest.id
    populateTemplateSummaryTable(triggered.metadata.templateManifest)

    // Create an invalid schedule record
    dynamoClient.putItem(
      new PutItemRequest(
        tableName,
        Map[String, AttributeValue](
          "scheduleId" -> new AttributeValue("scheduleId123"),
          "customerId" -> new AttributeValue(TestUtil.customerId),
          "status"     -> new AttributeValue("Pending"),
          "templateId" -> new AttributeValue(templateId)
        ).asJava
      )
    )
    val genericMetadata = GenericMetadataV3(triggered.metadata.createdAt,
                                            triggered.metadata.eventId,
                                            triggered.metadata.traceToken,
                                            triggered.metadata.commId,
                                            triggered.metadata.source,
                                            false)

    val cancellationRequested = CancellationRequestedV3(genericMetadata, templateId, TestUtil.customerId)

    Kafka.aiven.triggered.v4.publishOnce(triggered)
    Kafka.aiven.cancellationRequested.v3.publishOnce(cancellationRequested)

    val failedCancellation = failedCancellationConsumer.pollFor().head

    failedCancellation.reason shouldBe "Cancellation of scheduled comm failed: Failed to deserialise pending schedule"
    failedCancellation.cancellationRequested shouldBe cancellationRequested
    failedCancellation.metadata.source shouldBe "orchestration"

    cancelledConsumer.pollFor(noOfEventsExpected = 1)
    orchestratedEmailConsumer.checkNoMessages()
    expectFeedbackEvents(noOfEventsExpected = 3, consumer = feedbackConsumer, expectedStatuses = Set(FeedbackOptions.Scheduled, FeedbackOptions.Pending, FeedbackOptions.FailedCancellation))
  }

  it should "orchestrate emails requested to be sent in the future - triggeredV4" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedEmail.v4, Kafka.aiven.feedback.v1) { (orchestrationStartedConsumer, orchestratedEmailConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggeredV4.copy(deliverAt = Some(Instant.now().plusSeconds(2)))
    populateTemplateSummaryTable(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer)
    expectFeedbackEvents(noOfEventsExpected = 2, consumer = feedbackConsumer, expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Scheduled))
  }

  it should "orchestrate sms requested to be sent in the future" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestrationStarted.v3,
    Kafka.aiven.orchestratedSMS.v3,
    Kafka.aiven.feedback.v1
  ) { (orchestrationStartedConsumer, orchestratedSMSConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggeredV4.copy(
      deliverAt = Some(Instant.now().plusSeconds(1)),
      preferredChannels = Some(List(SMS))
    )

    populateTemplateSummaryTable(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectOrchestrationStartedEvents(noOfEventsExpected = 1, consumer = orchestrationStartedConsumer)


    orchestratedSMSConsumer
      .pollFor(noOfEventsExpected = 1)
      .foreach { ev =>
        ev.recipientPhoneNumber shouldBe "+447985631544"
        ev.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
        ev.templateData shouldBe triggered.templateData
    }
    expectFeedbackEvents(noOfEventsExpected = 2, consumer = feedbackConsumer, expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Scheduled))
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25.seconds,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true,
                                       consumer: KafkaConsumer[String, Option[OrchestrationStartedV3]]) = {
    val orchestrationStartedEvents =
      consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestrationStartedEvents.foreach { o =>
      o.metadata.templateManifest.id shouldBe Hash(TestUtil.commManifest.name)
    }
    orchestrationStartedEvents
  }

  def setCommId(cancellationRequest: CancellationRequestedV3) = {
    cancellationRequest.copy(metadata = cancellationRequest.metadata.copy(commId = "commId"))
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 25.seconds,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    consumer: KafkaConsumer[String, Option[OrchestratedEmailV4]]) = {
    val orchestratedEmails =
      consumer.pollFor(noOfEventsExpected = noOfEventsExpected)

    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      orchestratedEmail.metadata.templateManifest.id shouldBe Hash(TestUtil.commManifest.name)
      orchestratedEmail
    }
  }
}
