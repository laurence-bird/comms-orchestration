package servicetest

import java.time.{Instant, LocalDateTime}
import java.util.UUID

import servicetest.helpers._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest, PutItemResult}
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email._
import com.ovoenergy.comms.model.sms._
import com.ovoenergy.orchestration.domain._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory}

import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

//Implicits
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence._

class SchedulingServiceTest
    extends FlatSpec
    with DockerIntegrationTest
    with Matchers
    with MockProfileResponses
    with FakeS3Configuration
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.load("servicetest.conf")
  val mockServerClient        = new MockServerClient("localhost", 1080)
  val region                  = config.getString("aws.region")
  val s3Endpoint              = "http://localhost:4569"

  val uploadTemplate: (CommManifest) => Unit = uploadTemplateToFakeS3(region, s3Endpoint)

  lazy val orchestrationStartedConsumer = Kafka.aiven.orchestrationStarted.v2.consumer()
  lazy val orchestratedEmailConsumer    = Kafka.aiven.orchestratedEmail.v3.consumer()
  lazy val cancelledConsumer            = Kafka.aiven.cancelled.v2.consumer()
  lazy val orchestratedSMSConsumer      = Kafka.aiven.orchestratedSMS.v2.consumer()
  lazy val failedCancellationConsumer   = Kafka.aiven.failedCancellation.v2.consumer()

  private def allKafkaConsumers = Seq(
    orchestrationStartedConsumer,
    orchestratedEmailConsumer,
    cancelledConsumer,
    orchestratedSMSConsumer,
    failedCancellationConsumer
  )

  override def beforeAll() = {
    super.beforeAll()
    allKafkaConsumers.foreach(_.poll(5))
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  override def afterAll() = {
    allKafkaConsumers.foreach(_.close)
    super.afterAll()
  }

  behavior of "Legacy Comm Scheduling"

  it should "pick up expired events via polling and orchestrate them" in {
    createOKCustomerProfileResponse(mockServerClient)
    val schedulesTable = Table[Schedule](tableName)
    val triggered      = TestUtil.customerTriggered

    val deliverAt = Instant.now().minusSeconds(600)
    val expireAt  = Instant.now().minusSeconds(60)

    val result: PutItemResult = Scanamo.exec(dynamoClient)(
      schedulesTable.put(
        Schedule(
          "testSchedule",
          None,
          Some(triggered),
          deliverAt,
          ScheduleStatus.Orchestrating,
          Nil,
          expireAt,
          Some(TestUtil.customerId),
          triggered.metadata.commManifest.name
        )
      ))

    result.getSdkHttpMetadata.getHttpStatusCode shouldBe 200
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "pick up expired legacy events via polling and orchestrate them" in {
    createOKCustomerProfileResponse(mockServerClient)
    val schedulesTable = Table[Schedule](tableName)
    val triggered      = TestUtil.legacyTriggered

    val deliverAt = Instant.now().minusSeconds(600)
    val expireAt  = Instant.now().minusSeconds(60)

    val result: PutItemResult = Scanamo.exec(dynamoClient)(
      schedulesTable.put(
        Schedule(
          "testSchedule",
          Some(triggered),
          None,
          deliverAt,
          ScheduleStatus.Orchestrating,
          Nil,
          expireAt,
          Some(TestUtil.customerId),
          triggered.metadata.commManifest.name
        )
      ))

    result.getSdkHttpMetadata.getHttpStatusCode shouldBe 200
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "deschedule comms and generate cancelled events" in { //****//
    createOKCustomerProfileResponse(mockServerClient)
    val triggered1 = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(15)))
    val triggered2 = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(16)),
                                                     metadata = triggered1.metadata.copy(traceToken = "testTrace123"))

    // 2 trigger events for the same customer and comm
    val triggeredEvents = Seq(
      triggered1,
      triggered2
    )
    triggeredEvents.foreach(t => Kafka.legacy.triggered.v3.publishOnce(t))
    Thread.sleep(5000) // give it time to write the events to the DB

    val genericMetadata = GenericMetadataV2(createdAt = Instant.now(),
                                            eventId = UUID.randomUUID().toString,
                                            traceToken = UUID.randomUUID().toString,
                                            source = "service test",
                                            canary = false)
    val cancellationRequested: CancellationRequestedV2 =
      CancellationRequestedV2(genericMetadata, triggered1.metadata.commManifest.name, TestUtil.customerId)

    Kafka.legacy.cancellationRequested.v2.publishOnce(cancellationRequested)

    orchestrationStartedConsumer.checkNoMessages(3.second)
    orchestratedEmailConsumer.checkNoMessages(3.second)

    val cancelledComs = cancelledConsumer.pollFor(noOfEventsExpected = 2)
    cancelledComs.length shouldBe 2
    cancelledComs.map(cc => (cc.metadata.traceToken, cc.cancellationRequested)) should contain allOf (
      (triggered1.metadata.traceToken, cancellationRequested),
      (triggered2.metadata.traceToken, cancellationRequested)
    )
  }

  it should "legacy deschedule comms and generate cancelled events" in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered1 = TestUtil.legacyTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(15).toString))
    val triggered2 = TestUtil.legacyTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(16).toString),
                                                   metadata = triggered1.metadata.copy(traceToken = "testTrace123"))

    // 2 trigger events for the same customer and comm
    val triggeredEvents = Seq(
      triggered1,
      triggered2
    )

    triggeredEvents.foreach(t => Kafka.legacy.triggered.v2.publishOnce(t))

    Thread.sleep(5000) // give it time to write the events to the DB

    val genericMetadata = GenericMetadata(createdAt = Instant.now().toString,
                                          eventId = UUID.randomUUID().toString,
                                          traceToken = UUID.randomUUID().toString,
                                          source = "service test",
                                          canary = false)
    val cancellationRequested: CancellationRequested =
      CancellationRequested(genericMetadata, triggered1.metadata.commManifest.name, TestUtil.customerId)

    Kafka.legacy.cancellationRequested.v1.publishOnce(cancellationRequested)

    orchestrationStartedConsumer.checkNoMessages(3.second)
    orchestratedEmailConsumer.checkNoMessages(3.second)

    val cancelledComs = cancelledConsumer.pollFor(1000.millisecond, 2)
    cancelledComs.map(cc => (cc.metadata.traceToken, cc.cancellationRequested)) should contain allOf (
      (triggered1.metadata.traceToken, cancellationRequestedToV2(cancellationRequested)),
      (triggered2.metadata.traceToken, cancellationRequestedToV2(cancellationRequested))
    )
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
    Kafka.aiven.triggered.v3.publishOnce(triggered)
    Thread.sleep(500)
    Kafka.aiven.cancellationRequested.v2.publishOnce(cancellationRequested)

    val failedCancellation = failedCancellationConsumer.pollFor().head

    failedCancellation.reason shouldBe "Cancellation of scheduled comm failed: Failed to deserialise pending schedule"
    failedCancellation.cancellationRequested shouldBe cancellationRequested
    failedCancellation.metadata.source shouldBe "orchestration"

    cancelledConsumer.pollFor()
    orchestratedEmailConsumer.checkNoMessages()

  }

  it should "orchestrate emails requested to be sent in the future" in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(5)))
    Kafka.aiven.triggered.v3.publishOnce(triggered)
    orchestratedEmailConsumer.checkNoMessages(1.second)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "orchestrate legacy emails requested to be sent in the future" in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.legacyTriggered.copy(deliverAt = Some(Instant.now().plusSeconds(5).toString))
    Kafka.legacy.triggered.v2.publishOnce(triggered)

    orchestratedEmailConsumer.checkNoMessages(1.second)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "orchestrate sms requested to be sent in the future" in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.customerTriggered.copy(
      deliverAt = Some(Instant.now().plusSeconds(5)),
      preferredChannels = Some(List(SMS))
    )

    Kafka.aiven.triggered.v3.publishOnce(triggered)
    orchestratedSMSConsumer.checkNoMessages(1.seconds)
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    val orchestratedSMSEvents = orchestratedSMSConsumer.pollFor(30.seconds)

    orchestratedSMSEvents.foreach { ev =>
      ev.recipientPhoneNumber shouldBe "+447985631544"
      ev.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
      ev.templateData shouldBe triggered.templateData
    }
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 30.second,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    val orchestrationStartedEvents = orchestrationStartedConsumer.pollFor(pollTime, noOfEventsExpected)
    orchestrationStartedEvents.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestrationStartedEvents
  }

  def expectOrchestratedEmailEvents(pollTime: FiniteDuration = 30.second,
                                    noOfEventsExpected: Int,
                                    shouldCheckTraceToken: Boolean = true) = {
    val orchestratedEmails = orchestratedEmailConsumer.pollFor(pollTime, noOfEventsExpected)
    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe Some(model.CustomerProfile("John", "Wayne"))
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }
}
