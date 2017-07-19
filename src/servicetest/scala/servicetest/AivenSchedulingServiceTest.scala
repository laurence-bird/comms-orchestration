package servicetest

import java.time.Instant
import java.util.UUID

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest, PutItemResult}
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email._
import com.ovoenergy.comms.model.sms._
import com.ovoenergy.orchestration.domain._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import com.ovoenergy.orchestration.util.TestUtil
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import servicetest.helpers._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class AivenSchedulingServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with MockProfileResponses
    with FakeS3Configuration
    with ScalaFutures
    with BeforeAndAfterAll {

  import kafkaTesting._

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  val uploadTemplate: (CommManifest) => Unit = uploadTemplateToFakeS3(region, s3Endpoint)

  override def beforeAll() = {
    super.beforeAll()

    initKafkaConsumers()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  override def afterAll() = {
    shutdownAllKafkaProducers()
    shutdownAllKafkaConsumers()

    super.afterAll()
  }

  behavior of "Aiven comms scheduling"

  it should "deschedule comms and generate cancelled events" in { //****//
    createOKCustomerProfileResponse(mockServerClient)
    val triggered1 = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(20)))
    val triggered2 = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(21)),
                                                     metadata = triggered1.metadata.copy(traceToken = "testTrace123"))

    // 2 trigger events for the same customer and comm
    val triggeredEvents = Seq(
      triggered1,
      triggered2
    )
    val triggeredFuture = Future.sequence(triggeredEvents.map(tr =>
      aivenTriggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, tr))))
    Await.ready(triggeredFuture, atMost = 2.seconds)

    Thread.sleep(5000) // give it time to write the events to the DB

    val genericMetadata = GenericMetadataV2(createdAt = Instant.now(),
                                            eventId = UUID.randomUUID().toString,
                                            traceToken = UUID.randomUUID().toString,
                                            source = "service test",
                                            canary = false)
    val cancellationRequested: CancellationRequestedV2 =
      CancellationRequestedV2(genericMetadata, triggered1.metadata.commManifest.name, TestUtil.customerId)
    val cancelledFuture = aivenCancellationRequestedProducer.send(
      new ProducerRecord[String, CancellationRequestedV2](cancellationRequestTopic, cancellationRequested))

    whenReady(cancelledFuture) { _ =>
      orchestrationStartedConsumer.poll(5000).records(orchestrationStartedTopic).asScala.toList shouldBe empty
      orchestratedEmailConsumer.poll(2000).records(orchestratedEmailTopic).asScala.toList shouldBe empty

      val cancelledComs = pollForEvents(1000.millisecond, 2, cancelledConsumer, cancelledTopic)
      cancelledComs.length shouldBe 2
      cancelledComs.map(cc => (cc.metadata.traceToken, cc.cancellationRequested)) should contain allOf (
        (triggered1.metadata.traceToken, cancellationRequested),
        (triggered2.metadata.traceToken, cancellationRequested)
      )
    }
  }

  it should "generate a cancellationFailed event if unable to deschedule a comm" in {
    val triggered = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(15)))
    val commName  = triggered.metadata.commManifest.name

    // Create an invalid schedule record
    dynamoClient.putItem(
      new PutItemRequest(
        tableName,
        Map[String, AttributeValue](
          "scheduleId" -> new AttributeValue("scheduleId123"),
          "customerId" -> new AttributeValue(TestUtil.customerId),
          "status"     -> new AttributeValue("Pending"),
          "commName"   -> new AttributeValue(commName)
        ).asJava
      )
    )
    val genericMetadata = GenericMetadataV2(triggered.metadata.createdAt,
                                            triggered.metadata.eventId,
                                            triggered.metadata.traceToken,
                                            triggered.metadata.source,
                                            false)

    val cancellationRequested = CancellationRequestedV2(genericMetadata, commName, TestUtil.customerId)

    for {
      _ <- aivenTriggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
      _ <- aivenCancellationRequestedProducer.send(
        new ProducerRecord[String, CancellationRequestedV2](cancellationRequestTopic, cancellationRequested))
    } yield checkCancellations()

    def checkCancellations() = {
      val failedCancellations = failedCancellationConsumer.poll(20000).records(failedCancellationTopic).asScala.toList
      failedCancellations.length shouldBe 1
      failedCancellations.map(_.value().get) should contain(
        FailedCancellationV2(
          GenericMetadataV2.fromSourceGenericMetadata("orchestrated", cancellationRequested.metadata),
          cancellationRequested,
          "Cancellation of scheduled comm failed: Failed to deserialise pending schedule"
        ))

      val cancelledComs = pollForEvents(20000.millisecond, 1, cancelledConsumer, cancelledTopic)
      cancelledComs.length shouldBe 1
      val orchestratedComms = orchestratedEmailConsumer.poll(20000).records(orchestratedEmailTopic).asScala.toList
      orchestratedComms.length shouldBe 0
    }
  }

  it should "orchestrate emails requested to be sent in the future" in { //****//
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(5)))
    val future    = aivenTriggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
    whenReady(future) { _ =>
      val orchestratedEmails = orchestratedEmailConsumer.poll(1000).records(orchestratedEmailTopic).asScala.toList
      orchestratedEmails shouldBe empty
      expectOrchestrationStartedEvents(noOfEventsExpected = 1)
      expectOrchestratedEmailEvents(noOfEventsExpected = 1)
    }
  }

  it should "orchestrate sms requested to be sent in the future" in { //****//
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggered.copy(
      deliverAt = Some(Instant.now().plusSeconds(5)),
      preferredChannels = Some(List(SMS))
    )

    val future = aivenTriggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
    whenReady(future) { _ =>
      val orchestratedSMS = smsOrchestratedConsumer.poll(1000).records(orchestratedSmsTopic).asScala.toList
      orchestratedSMS shouldBe empty
      expectOrchestrationStartedEvents(noOfEventsExpected = 1)
      val orchestratedSMSEvents = pollForEvents[OrchestratedSMSV2](noOfEventsExpected = 1,
                                                                   consumer = smsOrchestratedConsumer,
                                                                   topic = orchestratedSmsTopic)

      orchestratedSMSEvents.foreach { ev =>
        ev.recipientPhoneNumber shouldBe "+447985631544"
        ev.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
        ev.templateData shouldBe triggered.templateData
      }
    }
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    val orchestrationStartedEvents = pollForEvents[OrchestrationStartedV2](pollTime,
                                                                           noOfEventsExpected,
                                                                           orchestrationStartedConsumer,
                                                                           orchestrationStartedTopic)
    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 25000.millisecond,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true) = {
    val orchestratedEmails = pollForEvents[OrchestratedEmailV3](pollTime,
                                                                noOfEventsExpected,
                                                                orchestratedEmailConsumer,
                                                                orchestratedEmailTopic)
    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }
}