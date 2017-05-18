package com.ovoenergy.orchestration.serviceTest

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EmailServiceTestIT
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
    super.beforeAll()
    kafkaTesting.setupTopics()
    uploadFragmentsToFakeS3(region, s3Endpoint)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
  }

  val mockServerClient = new MockServerClient("localhost", 1080)
  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  behavior of "Email Orchestration"

  // Upload valid template to S3 with both email and SMS templates available

  it should "orchestrate emails request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    val future =
      triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectOrchestratedEmailEvents(10000.millisecond, 1)
    }
  }

  it should "orchestrate legacy emails request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    val future = legacyTriggeredProducer.send(
      new ProducerRecord[String, TriggeredV2](legacyTriggeredTopic, TestUtil.legacyTriggered))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectOrchestratedEmailEvents(10000.millisecond, 1)
    }
  }

  it should "generate unique internalTraceTokens" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
    triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 2)
    val events = expectOrchestratedEmailEvents(noOfEventsExpected = 2)
    events match {
      case head :: tail =>
        head.internalMetadata.internalTraceToken should not equal tail.head.internalMetadata.internalTraceToken
    }
  }

  it should "orchestrate multiple emails" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)

    var futures = new mutable.ListBuffer[Future[_]]
    (1 to 10).foreach(counter => {
      val triggered =
        TestUtil.customerTriggered.copy(metadata = TestUtil.metadataV2.copy(traceToken = counter.toString))
      futures += triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
    })
    futures.foreach(future => Await.ready(future, 1.seconds))

    val deadline = 15.seconds
    val orchestrationStartedEvents =
      expectOrchestrationStartedEvents(noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    val orchestratedEmails =
      expectOrchestratedEmailEvents(pollTime = deadline, noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestratedEmails.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  it should "raise failure for customers with insufficient details to orchestrate emails for" taggedAs DockerComposeTag in {
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadata.commManifest)
    createInvalidCustomerProfileResponse(mockServerClient)
    val future =
      triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    whenReady(future) { _ =>
      val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
      failures.size shouldBe 1
      failures.foreach(record => {
        val failure = record.value().getOrElse(fail(s"No record for ${record.key()}"))
        failure.reason should include("No contact details found")
        failure.errorCode shouldBe InvalidProfile
        failure.metadata.traceToken shouldBe TestUtil.traceToken
      })
    }
  }

  it should "raise failure when customer profiler fails" taggedAs DockerComposeTag in {
    createBadCustomerProfileResponse(mockServerClient)

    val future =
      triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
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

    triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.customerTriggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    expectOrchestratedEmailEvents(noOfEventsExpected = 1)
  }

  it should "orchestrate triggered event with email contact details" taggedAs DockerComposeTag in {
    val future =
      triggeredProducer.send(
        new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.emailContactDetailsTriggered))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectOrchestratedEmailEvents(10000.millisecond, 1, shouldHaveCustomerProfile = false)
    }
  }

  it should "raise failure for triggered event with contact details with insufficient details" taggedAs DockerComposeTag in {
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.metadata.commManifest)
    val future =
      triggeredProducer.send(
        new ProducerRecord[String, TriggeredV3](triggeredTopic, TestUtil.invalidContactDetailsTriggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    whenReady(future) { _ =>
      val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
      failures.size shouldBe 1
      failures.foreach(record => {
        val failure = record.value().getOrElse(fail(s"No record for ${record.key()}"))
        failure.reason should include("No contact details found")
        failure.errorCode shouldBe InvalidProfile
        failure.metadata.traceToken shouldBe TestUtil.traceToken
      })
    }
  }

  it should "raise failure for triggered event with contact details that do not provide details for template channel" taggedAs DockerComposeTag in {
    val commManifest = CommManifest(Service, "sms-only", "0.1")
    val metadata = TestUtil.metadataV2.copy(
      deliverTo = ContactDetails(Some("qatesting@ovoenergy.com"), None),
      commManifest = commManifest
    )
    val triggered = TestUtil.emailContactDetailsTriggered.copy(metadata = metadata)

    uploadSMSOnlyTemplateToFakeS3(region, s3Endpoint)(commManifest)

    val future =
      triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
    expectOrchestrationStartedEvents(noOfEventsExpected = 1)
    whenReady(future) { _ =>
      val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
      failures.size shouldBe 1
      failures.foreach(record => {
        val failure = record.value().getOrElse(fail(s"No record for ${record.key()}"))
        failure.reason should include("No available channels to deliver comm")
        failure.errorCode shouldBe OrchestrationError
        failure.metadata.traceToken shouldBe TestUtil.traceToken
      })
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
                                    shouldCheckTraceToken: Boolean = true,
                                    shouldHaveCustomerProfile: Boolean = true) = {
    val orchestratedEmails = pollForEvents[OrchestratedEmailV3](pollTime,
                                                                noOfEventsExpected,
                                                                orchestratedEmailConsumer,
                                                                emailOrchestratedTopic)
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
