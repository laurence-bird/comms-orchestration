package com.ovoenergy.orchestration.serviceTest

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.Channel.SMS
import com.ovoenergy.comms.model.{OrchestratedEmailV2, OrchestratedSMS, OrchestrationStarted, TriggeredV2}
import com.ovoenergy.orchestration.serviceTest.util.{DynamoTesting, FakeS3Configuration, KafkaTesting, MockProfileResponses}
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

class ServiceTestSMS
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

  val region           = config.getString("aws.region")
  val s3Endpoint       = "http://localhost:4569"

  object DockerComposeTag extends Tag("DockerComposeTag")

  override def beforeAll() = {
    createTable()
    setupTopics()
  }

  behavior of "SMS Orchestration"

  it should "orchestrate SMS request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)
    uploadTemplateToS3(region, s3Endpoint)(TestUtil.triggered.metadata.commManifest)
    val triggerSMS = TestUtil.triggered.copy(preferredChannels=Some(List(SMS)))
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggerSMS))
    whenReady(future) { _ =>
      expectOrchestrationStartedEvents(10000.millisecond, 1)
      expectSMSOrchestrationEvents(10000.millisecond, 1)
    }
  }

  it should "orchestrate multiple SMS" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse(mockServerClient)

    var futures = new mutable.ListBuffer[Future[_]]
    (1 to 10).foreach(counter => {
      val triggered = TestUtil.triggered.copy(metadata = TestUtil.metadata.copy(traceToken = counter.toString), preferredChannels=Some(List(SMS)))
      futures += triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
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

  def expectOrchestrationStartedEvents(pollTime: FiniteDuration = 20000.millisecond,
                                       noOfEventsExpected: Int,
                                       shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[OrchestrationStarted]): Seq[OrchestrationStarted] = {
      if (deadline.hasTimeLeft) {
        val eventsThisPoll = orchestrationStartedConsumer
          .poll(1000)
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

  def expectSMSOrchestrationEvents(pollTime: FiniteDuration = 20000.millisecond,
                                   noOfEventsExpected: Int,
                                   shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, emails: Seq[OrchestratedSMS]): Seq[OrchestratedSMS] = {
      if (deadline.hasTimeLeft) {
        val orchestratedSMS = smsOrchestratedConsumer
          .poll(1000)
          .records(smsOrchestratedTopic)
          .asScala
          .toList
          .flatMap(_.value())
        val smsSoFar = orchestratedSMS ++ emails
        smsSoFar.length match {
          case n if n == noOfEventsExpected => smsSoFar
          case exceeded if exceeded > noOfEventsExpected =>
            fail(s"Consumed more than $noOfEventsExpected orchestrated SMS event")
          case _ => poll(deadline, smsSoFar)
        }
      } else fail("SMS was not orchestrated within time limit")
    }
    val orchestratedSMS = poll(pollTime.fromNow, Nil)
    orchestratedSMS.map { orchestratedEmail =>
      orchestratedEmail.customerProfile shouldBe model.CustomerProfile("Gary", "Philpott")
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if (shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }
}
