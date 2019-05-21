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
import com.ovoenergy.orchestration.util.{ArbGenerator, ArbInstances, TestUtil}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.mockserver.client.server.MockServerClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import servicetest.helpers._
import monocle.macros.syntax.lens._
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SchedulingServiceTest
    extends BaseSpec
    with MockProfileResponses
    with KafkaTesting
    with FakeS3Configuration
    with ScalaFutures
    with ArbGenerator
    with BeforeAndAfterAll {

  implicit val config = ConfigFactory.load("servicetest.conf")

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  val uploadTemplate: (TemplateManifest) => Unit = uploadTemplateToFakeS3(region, s3Endpoint)

  override def beforeAll() = {
    super.beforeAll()
    uploadFragmentsToFakeS3(region, s3Endpoint)
  }

  behavior of "Aiven comms scheduling"

  it should "orchestrate emails requested to be sent in the future - triggeredV4" in withMultipleThrowawayConsumersFor(
    Kafka.aiven.orchestratedEmail.v4,
    Kafka.aiven.feedback.v1) { (orchestratedEmailConsumer, feedbackConsumer) =>
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggeredV4.copy(deliverAt = Some(Instant.now().plusSeconds(1)))
    populateTemplateSummaryTable(triggered.metadata.templateManifest)
    uploadTemplateToFakeS3(region, s3Endpoint)(triggered.metadata.templateManifest)

    Kafka.aiven.triggered.v4.publishOnce(triggered)

    expectOrchestratedEmailEvents(noOfEventsExpected = 1, consumer = orchestratedEmailConsumer, triggered = triggered)
    expectFeedbackEvents(noOfEventsExpected = 1,
                         consumer = feedbackConsumer,
                         expectedStatuses = Set(FeedbackOptions.Pending, FeedbackOptions.Scheduled))
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 25.seconds,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true,
                                    consumer: KafkaConsumer[String, OrchestratedEmailV4],
                                    triggered: TriggeredV4) = {
    val orchestratedEmails =
      consumer.pollFor(noOfEventsExpected = noOfEventsExpected)
  }
}
