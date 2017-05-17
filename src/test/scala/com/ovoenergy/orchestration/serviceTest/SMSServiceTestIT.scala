package com.ovoenergy.orchestration.serviceTest

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.sms.OrchestratedSMSV2
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
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class SMSServiceTestIT
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with DynamoTesting
    with FakeS3Configuration {

  val config: Config =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))
  val kafkaTesting = new KafkaTesting(config)
  import kafkaTesting._
  val mockServerClient = new MockServerClient("localhost", 1080)

  val region     = config.getString("aws.region")
  val s3Endpoint = "http://localhost:4569"

  object DockerComposeTag extends Tag("DockerComposeTag")

  override def beforeAll() = {
    super.beforeAll()
    setupTopics()
    kafkaTesting.setupTopics()
  }

  behavior of "SMS Orchestration"

  it should "orchestrate SMS request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToFakeS3(region, s3Endpoint)(TestUtil.customerTriggered.metadata.commManifest)
    val triggerSMS = TestUtil.customerTriggered.copy(preferredChannels = Some(List(SMS)))
    val future     = triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggerSMS))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectSMSOrchestrationEvents(10000.millisecond, 1)
    }
  }

  it should "orchestrate multiple SMS" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)

    var futures = new mutable.ListBuffer[Future[_]]
    (1 to 10).foreach(counter => {
      val triggered = TestUtil.customerTriggered.copy(metadata =
                                                        TestUtil.metadataV2.copy(traceToken = counter.toString),
                                                      preferredChannels = Some(List(SMS)))
      futures += triggeredProducer.send(new ProducerRecord[String, TriggeredV3](triggeredTopic, triggered))
    })
    futures.foreach(future => Await.ready(future, 1.seconds))

    val deadline = 15.seconds
    val orchestrationStartedEvents =
      expectOrchestrationStartedEvents(noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestrationStartedEvents.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    val orchestratedSMSes =
      expectSMSOrchestrationEvents(pollTime = deadline, noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestratedSMSes.map(_.metadata.traceToken) should contain allOf ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
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

  def expectSMSOrchestrationEvents(pollTime: FiniteDuration = 20000.millisecond,
                                   noOfEventsExpected: Int,
                                   shouldCheckTraceToken: Boolean = true) = {
    val orchestratedSMS =
      pollForEvents[OrchestratedSMSV2](pollTime, noOfEventsExpected, smsOrchestratedConsumer, smsOrchestratedTopic)
    orchestratedSMS.map { orchestratedSMS =>
      orchestratedSMS.customerProfile shouldBe Some(CustomerProfile("John", "Wayne"))
      orchestratedSMS.templateData shouldBe TestUtil.templateData
      if (shouldCheckTraceToken) orchestratedSMS.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedSMS
    }
  }
}
