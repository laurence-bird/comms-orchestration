package com.ovoenergy.orchestration.serviceTest

import java.time.Instant

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.ErrorCode.{InvalidProfile, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence._
import com.ovoenergy.orchestration.serviceTest.util.{
  DynamoTesting,
  FakeS3Configuration,
  KafkaTesting,
  MockProfileResponses
}
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ServiceTestIT
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with FakeS3Configuration
    with DynamoTesting {

  object DockerComposeTag extends Tag("DockerComposeTag")

  val config: Config =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))
  val kafkaTesting = new KafkaTesting(config)
  import kafkaTesting._

  override def beforeAll() = {
    createTable()
    kafkaTesting.setupTopics()
    uploadTemplateToS3(region, s3Endpoint)(TestUtil.triggered.metadata.commManifest)
  }

  override def afterAll() = {
    dynamoClient.deleteTable(tableName)
  }

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  behavior of "Service Testing"

  // Upload valid template to S3 with both email and SMS templates available

  it should "orchestrate emails request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectOrchestrationEvents(10000.millisecond, 1)
    }
  }

  it should "generate unique internalTraceTokens" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 2)
    val events = expectOrchestrationEvents(noOfEventsExpected = 2)
    events match {
      case head :: tail =>
        head.internalMetadata.internalTraceToken should not equal tail.head.internalMetadata.internalTraceToken
    }
  }
  it should "orchestrate multiple emails" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)

    var futures = new mutable.ListBuffer[Future[_]]
    (1 to 10).foreach(counter => {
      val triggered = TestUtil.triggered.copy(metadata = TestUtil.metadata.copy(traceToken = counter.toString))
      futures += triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    })
    futures.foreach(future => Await.ready(future, 1.seconds))

    val deadline = 15.seconds
    val orchestrationStartedEvents =
      expectOrchestrationStartedEvents(noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    val orchestratedEmails =
      expectOrchestrationEvents(pollTime = deadline, noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestratedEmails.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  it should "raise failure for customers with insufficient details to orchestrate emails for" taggedAs DockerComposeTag in {
    uploadTemplateToS3(region, s3Endpoint)(TestUtil.metadata.commManifest)
    createInvalidCustomerProfileResponse(mockServerClient)
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    whenReady(future) { _ =>
      val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
      failures.size shouldBe 1
      failures.foreach(record => {
        val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
        failure.reason should include("No contact details found on customer profile")
        failure.errorCode shouldBe InvalidProfile
        failure.metadata.traceToken shouldBe TestUtil.traceToken
      })
    }
  }

  it should "raise failure when customer profiler fails" taggedAs DockerComposeTag in {
    createBadCustomerProfileResponse(mockServerClient)

    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    whenReady(future) { _ =>
      val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
      failures.size shouldBe 1
      failures.foreach(record => {
        val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
        failure.reason should include("Error response (500) from profile service: Some error")
        failure.metadata.traceToken shouldBe TestUtil.traceToken
        failure.errorCode shouldBe ProfileRetrievalFailed
      })
    }
  }

  it should "retry if the profile service returns an error response" taggedAs DockerComposeTag in {
    createFlakyCustomerProfileResponse(mockServerClient)

    triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestrationEvents(noOfEventsExpected = 1)
  }

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 25000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[OrchestrationStarted]): Seq[OrchestrationStarted] = {
      if (deadline.hasTimeLeft) {
        val eventsThisPoll = orchestrationStartedConsumer
          .poll(500)
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

  def expectOrchestrationEvents(pollTime: FiniteDuration = 25000.millisecond,
                                noOfEventsExpected: Int,
                                shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, emails: Seq[OrchestratedEmailV2]): Seq[OrchestratedEmailV2] = {
      if (deadline.hasTimeLeft) {
        val orchestratedEmails = emailOrchestratedConsumer
          .poll(500)
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
