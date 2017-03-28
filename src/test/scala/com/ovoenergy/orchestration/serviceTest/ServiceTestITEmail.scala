package com.ovoenergy.orchestration.serviceTest

import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.{OrchestratedEmailV2, OrchestrationStarted, TriggeredV2}
import com.ovoenergy.orchestration.serviceTest.util.{DynamoTesting, KafkaTesting, MockProfileResponses}
import com.ovoenergy.orchestration.util.TestUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.client.server.MockServerClient
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.duration._

class ServiceTestITEmail
    extends FlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with IntegrationPatience
    with MockProfileResponses
    with DynamoTesting {

  val config: Config =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))
  val kafkaTesting = new KafkaTesting(config)
  import kafkaTesting._
  val mockServerClient = new MockServerClient("localhost", 1080)

  object DockerComposeTag extends Tag("DockerComposeTag")

  override def beforeAll() = {
    createTable()
    setupTopics()
  }

  behavior of "Email"
  it should "Invoke this test" taggedAs DockerComposeTag in {
    1 shouldBe 1
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

  def expectOrchestrationEvents(pollTime: FiniteDuration = 20000.millisecond,
                                noOfEventsExpected: Int,
                                shouldCheckTraceToken: Boolean = true) = {
    @tailrec
    def poll(deadline: Deadline, emails: Seq[OrchestratedEmailV2]): Seq[OrchestratedEmailV2] = {
      if (deadline.hasTimeLeft) {
        val orchestratedEmails = emailOrchestratedConsumer
          .poll(1000)
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
