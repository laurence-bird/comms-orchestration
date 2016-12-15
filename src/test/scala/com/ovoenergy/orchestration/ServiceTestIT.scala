package com.ovoenergy.orchestration

import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.orchestration.util.TestUtil
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal

class ServiceTestIT extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll {

  object DockerComposeTag extends Tag("DockerComposeTag")

  override def beforeAll() = {
    setupTopics()
  }

  val kafkaHosts = "localhost:29092"
  val zookeeperHosts = "localhost:32181"

  val consumerGroup = Random.nextString(10)
  val triggeredProducer = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[Triggered], kafkaHosts))
  val commFailedConsumer = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[Failed], kafkaHosts, consumerGroup))
  val emailOrchestratedConsumer = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedEmail], kafkaHosts, consumerGroup))

  val failedTopic = "comms.failed"
  val triggeredTopic = "comms.triggered"
  val emailOrchestratedTopic = "comms.orchestrated.email"

  behavior of "Service Testing"

  it should "orchestrate emails" taggedAs DockerComposeTag in {

    val future = triggeredProducer.send(new ProducerRecord[String, Triggered](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      case _ =>
        val orchestratedEmails = emailOrchestratedConsumer.poll(30000).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails.size shouldBe 1
        orchestratedEmails.foreach(record => {
          val orchestratedEmail = record.value().getOrElse(fail("No record for ${record.key()}"))
          orchestratedEmail.recipientEmailAddress shouldBe "some.email@ovoenergy.com"
          orchestratedEmail.customerProfile shouldBe model.CustomerProfile("John", "Smith")
          orchestratedEmail.templateData shouldBe TestUtil.templateData
          orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  it should "raise failure for customers with insufficient details to orchestrate emails for" taggedAs DockerComposeTag in {

    val badMetaData = TestUtil.metadata.copy(customerId = "invalidCustomer")
    val badTriggered = TestUtil.triggered.copy(metadata = badMetaData)

    val future = triggeredProducer.send(new ProducerRecord[String, Triggered](triggeredTopic, badTriggered))
    whenReady(future) {
      case _ =>
        val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
        failures.size shouldBe 1
        failures.foreach(record => {
          val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
          failure.reason should include("Customer has no email address")
          failure.reason should include("Customer has no last name")
          failure.reason should include("Customer has no first name")
          failure.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  it should "raise failure when customer profiler fails" taggedAs DockerComposeTag in {

    val badMetaData = TestUtil.metadata.copy(customerId = "errorCustomer")
    val badTriggered = TestUtil.triggered.copy(metadata = badMetaData)

    val future = triggeredProducer.send(new ProducerRecord[String, Triggered](triggeredTopic, badTriggered))
    whenReady(future) {
      case _ =>
        val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
        failures.size shouldBe 1
        failures.foreach(record => {
          val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
          failure.reason shouldBe "Orchestration failed: Some failure reason"
          failure.metadata.traceToken shouldBe TestUtil.traceToken
        })
    }
  }

  def setupTopics() {
    import _root_.kafka.admin.AdminUtils
    import _root_.kafka.utils.ZkUtils

    import scala.concurrent.duration._

    val zkUtils = ZkUtils(zookeeperHosts, 30000, 5000, isZkSecurityEnabled = false)

    //Wait until kafka calls are not erroring and the service has created the triggeredTopic
    val timeout = 10.seconds.fromNow
    var notStarted = true
    while (timeout.hasTimeLeft && notStarted) {
      try {
        notStarted = !AdminUtils.topicExists(zkUtils, triggeredTopic)
      } catch {
        case NonFatal(ex) => Thread.sleep(100)
      }
    }
    Thread.sleep(3000L)
    if (notStarted) fail("Services did not start within 10 seconds")

    if (!AdminUtils.topicExists(zkUtils, failedTopic)) AdminUtils.createTopic(zkUtils, failedTopic, 1, 1)
    if (!AdminUtils.topicExists(zkUtils, emailOrchestratedTopic)) AdminUtils.createTopic(zkUtils, emailOrchestratedTopic, 1, 1)
    commFailedConsumer.assign(Seq(new TopicPartition(failedTopic, 0)).asJava)
    commFailedConsumer.poll(5000).records(failedTopic).asScala.toList
    emailOrchestratedConsumer.assign(Seq(new TopicPartition(emailOrchestratedTopic, 0)).asJava)
    emailOrchestratedConsumer.poll(5000).records(emailOrchestratedTopic).asScala.toList
  }


}
