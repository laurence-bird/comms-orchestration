package com.ovoenergy.orchestration.serviceTest

import java.time.Instant

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest, PutItemResult}
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import com.ovoenergy.orchestration.serviceTest.util.{
  DynamoTesting,
  FakeS3Configuration,
  KafkaTesting,
  MockProfileResponses
}
import com.ovoenergy.orchestration.util.TestUtil
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence._
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class SchedulingServiceTestIT
    extends FlatSpec
    with Matchers
    with MockProfileResponses
    with FakeS3Configuration
    with DynamoTesting
    with ScalaFutures
    with BeforeAndAfterAll {

  object DockerComposeTag extends Tag("DockerComposeTag")
  val config: Config =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))
  val kafkaTesting = new KafkaTesting(config)
  import kafkaTesting._

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  val uploadTemplate: (CommManifest) => Unit = uploadTemplateToS3(region, s3Endpoint)

  override def beforeAll() = {
    uploadTemplate(TestUtil.triggered.metadata.commManifest)
    createTable()
    kafkaTesting.setupTopics()
  }

  override def afterAll() = {
    dynamoClient.deleteTable(tableName)
  }

  behavior of "Comm Scheduling"

//  it should "pick up expired events via polling and orchestrate them" taggedAs DockerComposeTag in {
//    createOKCustomerProfileResponse(mockServerClient)
//    val schedulesTable = Table[Schedule](tableName)
//    val triggered      = TestUtil.triggered
//
//    val deliverAt = Instant.now().minusSeconds(600)
//    val expireAt  = Instant.now().minusSeconds(60)
//
//    val result: PutItemResult = Scanamo.exec(dynamoClient)(
//      schedulesTable.put(
//        Schedule(
//          "testSchedule",
//          triggered,
//          deliverAt,
//          ScheduleStatus.Orchestrating,
//          Nil,
//          expireAt,
//          triggered.metadata.customerId,
//          triggered.metadata.commManifest.name
//        )
//      ))
//
//    println(result.getSdkHttpMetadata.getHttpStatusCode)
//    println(result.getSdkHttpMetadata.getHttpHeaders)
//    println(result.getSdkResponseMetadata.toString)
//    println(result.getSdkResponseMetadata.getRequestId)
//    result.getSdkHttpMetadata.getHttpStatusCode shouldBe 200
//    expectOrchestrationStartedEvents(noOfEventsExpected = 1, pollTime = 25000.millisecond)
//    expectOrchestrationEvents(noOfEventsExpected = 1, pollTime = 25000.millisecond)
//  }

  it should "deschedule comms and generate cancelled events" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered1 = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(20).toString))
    val triggered2 = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(21).toString),
                                             metadata = triggered1.metadata.copy(traceToken = "testTrace123"))

    // 2 trigger events for the same customer and comm
    val triggeredEvents = Seq(
      triggered1,
      triggered2
    )
    val genericMetadata = GenericMetadata(triggeredEvents.head.metadata.createdAt,
                                          triggered1.metadata.eventId,
                                          triggered1.metadata.traceToken,
                                          triggered1.metadata.source,
                                          false)
    val cancellationRequested: CancellationRequested =
      CancellationRequested(genericMetadata, triggered1.metadata.commManifest.name, triggered1.metadata.customerId)

    val triggeredFuture = Future.sequence(
      triggeredEvents.map(tr => triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, tr))))
    Thread.sleep(100)
    val cancelledFuture = triggeredFuture.flatMap(
      t =>
        cancelationRequestedProducer.send(
          new ProducerRecord[String, CancellationRequested](cancellationRequestTopic, cancellationRequested)))
    whenReady(cancelledFuture) { _ =>
      orchestrationStartedConsumer.poll(15000).records(orchestrationStartedTopic).asScala.toList shouldBe empty
      emailOrchestratedConsumer.poll(2000).records(emailOrchestratedTopic).asScala.toList shouldBe empty

      val cancelledComs = cancelledConsumer.poll(20000).records(cancelledTopic).asScala.toList
      cancelledComs.length shouldBe 2
      cancelledComs.map(record => (record.value().get.metadata.traceToken, record.value().get.cancellationRequested)) should contain allOf (
        (triggered1.metadata.traceToken, cancellationRequested),
        (triggered2.metadata.traceToken, cancellationRequested)
      )
    }
  }

//  it should "generate a cancellationFailed event if unable to deschedule a comm" taggedAs DockerComposeTag in {
//    val triggered  = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(15).toString))
//    val commName   = triggered.metadata.commManifest.name
//    val customerId = triggered.metadata.customerId
//
//    // Create an invalid schedule record
//    dynamoClient.putItem(
//      new PutItemRequest(
//        tableName,
//        Map[String, AttributeValue](
//          "scheduleId" -> new AttributeValue("scheduleId123"),
//          "customerId" -> new AttributeValue(customerId),
//          "status"     -> new AttributeValue("Pending"),
//          "commName"   -> new AttributeValue(commName)
//        ).asJava
//      )
//    )
//    val genericMetadata = GenericMetadata(triggered.metadata.createdAt,
//                                          triggered.metadata.eventId,
//                                          triggered.metadata.traceToken,
//                                          triggered.metadata.source,
//                                          false)
//
//    val cancellationRequested = CancellationRequested(genericMetadata, commName, customerId)
//
//    for {
//      _ <- triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
//      _ <- cancelationRequestedProducer.send(
//        new ProducerRecord[String, CancellationRequested](cancellationRequestTopic, cancellationRequested))
//    } yield checkCancellations()
//
//    def checkCancellations() = {
//      val failedCancellations = failedCancellationConsumer.poll(20000).records(failedCancellationTopic).asScala.toList
//      failedCancellations.length shouldBe 1
//      failedCancellations.map(_.value().get) should contain(
//        FailedCancellation(
//          GenericMetadata.fromSourceGenericMetadata("orchestrated", cancellationRequested.metadata),
//          cancellationRequested,
//          "Cancellation of scheduled comm failed: Failed to deserialise pending schedule"
//        ))
//      val cancelledComs = cancelledConsumer.poll(20000).records(cancelledTopic).asScala.toList
//      cancelledComs.length shouldBe 1
//      val orchestratedComms = emailOrchestratedConsumer.poll(20000).records(emailOrchestratedTopic).asScala.toList
//      orchestratedComms.length shouldBe 0
//    }
//  }

  it should "orchestrate emails requested to be sent in the future" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    val triggered = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(5).toString))
    val future    = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    whenReady(future) { _ =>
      val orchestratedEmails = emailOrchestratedConsumer.poll(1000).records(emailOrchestratedTopic).asScala.toList
      orchestratedEmails shouldBe empty
      expectOrchestrationStartedEvents(noOfEventsExpected = 1)
      expectOrchestrationEvents(noOfEventsExpected = 1)
    }
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 20000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[OrchestrationStarted]): Seq[OrchestrationStarted] = {
      if (deadline.hasTimeLeft) {
        val eventsThisPoll = orchestrationStartedConsumer
          .poll(100)
          .records(orchestrationStartedTopic)
          .asScala
          .toList
          .flatMap(_.value())
        val eventsSoFar = events ++ eventsThisPoll
        eventsSoFar.length match {
          case n if n == noOfEventsExpected => eventsSoFar
          case exceeded if exceeded > noOfEventsExpected =>
            fail(s"Consumed more than $noOfEventsExpected events")
          case _ => poll(deadline, eventsSoFar)
        }
      } else throw new Exception("Comm orchestration not started within time limit")
    }
    val orchestratedEmails = poll(pollTime.fromNow, Nil)
    orchestratedEmails.foreach { o =>
      if (shouldCheckTraceToken) o.metadata.traceToken shouldBe TestUtil.traceToken
    }
    orchestratedEmails
  }

  def expectOrchestrationEvents(pollTime: FiniteDuration = 20000.millisecond,
                                noOfEventsExpected: Int,
                                shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, emails: Seq[OrchestratedEmailV2]): Seq[OrchestratedEmailV2] = {
      if (deadline.hasTimeLeft) {
        val orchestratedEmails = emailOrchestratedConsumer
          .poll(100)
          .records(emailOrchestratedTopic)
          .asScala
          .toList
          .flatMap(_.value())
        val emailsSoFar = orchestratedEmails ++ emails
        emailsSoFar.length match {
          case n if n == noOfEventsExpected => emailsSoFar
          case exceeded if exceeded > noOfEventsExpected =>
            fail(s"Consumed more than $noOfEventsExpected orchestrated email event")
          case _ => poll(deadline, emailsSoFar)
        }
      } else fail("Email was not orchestrated within time limit")
    }
    val orchestratedEmails = poll(pollTime.fromNow, Nil)
    orchestratedEmails.map { orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe model.CustomerProfile("Gary", "Philpott")
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }

}
