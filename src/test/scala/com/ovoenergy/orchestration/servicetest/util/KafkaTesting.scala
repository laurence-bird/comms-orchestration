package com.ovoenergy.orchestration.serviceTest.util

import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email._
import com.ovoenergy.comms.model.sms._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer => ApacheKafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Random
import scala.util.control.NonFatal
import io.circe.generic.auto._
import com.ovoenergy.comms.serialisation.Codecs._
import kafka.utils.ZkUtils

class KafkaTesting(config: Config) {
  val kafkaHosts     = "localhost:29092"
  val zookeeperHosts = "localhost:32181"

  val consumerGroup = Random.nextString(10)

  val legacyTriggeredProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[TriggeredV2], kafkaHosts))
  val legacyCancelationRequestedProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[CancellationRequested], kafkaHosts))

  val cancelationRequestedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[CancellationRequestedV2], kafkaHosts, consumerGroup))
  val cancelledConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[CancelledV2], kafkaHosts, consumerGroup))
  val commFailedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[FailedV2], kafkaHosts, consumerGroup))
  val orchestratedEmailConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedEmailV3], kafkaHosts, consumerGroup))
  val smsOrchestratedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedSMSV2], kafkaHosts, consumerGroup))
  val orchestrationStartedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestrationStartedV2], kafkaHosts, consumerGroup))
  val failedCancellationConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[FailedCancellationV2], kafkaHosts, consumerGroup))

  val triggeredProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[TriggeredV3], kafkaHosts))

  val cancelationRequestedProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[CancellationRequestedV2], kafkaHosts))

  val failedTopic               = config.getString("kafka.topics.failed.v2")
  val triggeredTopic            = config.getString("kafka.topics.triggered.v3")
  val cancellationRequestTopic  = config.getString("kafka.topics.scheduling.cancellationRequest.v2")
  val cancelledTopic            = config.getString("kafka.topics.scheduling.cancelled.v2")
  val failedCancellationTopic   = config.getString("kafka.topics.scheduling.failedCancellation.v2")
  val emailOrchestratedTopic    = config.getString("kafka.topics.orchestrated.email.v3")
  val smsOrchestratedTopic      = config.getString("kafka.topics.orchestrated.sms.v2")
  val orchestrationStartedTopic = config.getString("kafka.topics.orchestration.started.v2")

  val legacyTriggeredTopic           = config.getString("kafka.topics.triggered.v2")
  val legacyCancellationRequestTopic = config.getString("kafka.topics.scheduling.cancellationRequest.v1")

  val topics = Seq(
    failedTopic,
    triggeredTopic,
    cancellationRequestTopic,
    cancelledTopic,
    failedCancellationTopic,
    emailOrchestratedTopic,
    smsOrchestratedTopic,
    orchestrationStartedTopic
  )

  def createTopic(topicName: String, zkUtils: ZkUtils) {
    import _root_.kafka.admin.AdminUtils
    import _root_.kafka.utils.ZkUtils

    import scala.concurrent.duration._
    if (!AdminUtils.topicExists(zkUtils, topicName)) {
      AdminUtils.createTopic(zkUtils, topicName, 1, 1)

      val timeout    = 10.seconds.fromNow
      var notStarted = true
      while (timeout.hasTimeLeft && notStarted) {
        try {
          notStarted = !AdminUtils.topicExists(zkUtils, topicName)
        } catch {
          case NonFatal(ex) => Thread.sleep(100)
        }
      }
      if (notStarted) throw new Exception("Kafka services did not start within 10 seconds")
    }
    //Wait until kafka calls are not erroring and the service has created the composedEmailTopic
  }

  def setupTopics() {
    import _root_.kafka.admin.AdminUtils
    import _root_.kafka.utils.ZkUtils

    import scala.concurrent.duration._

    val zkUtils = ZkUtils(zookeeperHosts, 30000, 5000, isZkSecurityEnabled = false)

    topics.foreach { topic =>
      createTopic(topic, zkUtils)
    }

    failedCancellationConsumer.assign(Seq(new TopicPartition(failedCancellationTopic, 0)).asJava)
    failedCancellationConsumer.poll(5000)
    orchestrationStartedConsumer.assign(Seq(new TopicPartition(orchestrationStartedTopic, 0)).asJava)
    orchestrationStartedConsumer.poll(5000)
    commFailedConsumer.assign(Seq(new TopicPartition(failedTopic, 0)).asJava)
    commFailedConsumer.poll(5000)
    orchestratedEmailConsumer.assign(Seq(new TopicPartition(emailOrchestratedTopic, 0)).asJava)
    orchestratedEmailConsumer.poll(5000)
    smsOrchestratedConsumer.assign(Seq(new TopicPartition(smsOrchestratedTopic, 0)).asJava)
    smsOrchestratedConsumer.poll(5000)
    cancelationRequestedConsumer.assign(Seq(new TopicPartition(cancellationRequestTopic, 0)).asJava)
    cancelationRequestedConsumer.poll(5000)
    cancelledConsumer.assign(Seq(new TopicPartition(cancelledTopic, 0)).asJava)
    cancelledConsumer.poll(5000)

  }

  def pollForEvents[E](pollTime: FiniteDuration = 20000.millisecond,
                       noOfEventsExpected: Int,
                       consumer: ApacheKafkaConsumer[String, Option[E]],
                       topic: String): Seq[E] = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[E]): Seq[E] = {
      if (deadline.hasTimeLeft) {
        val polledEvents: Seq[E] = consumer
          .poll(100)
          .records(topic)
          .asScala
          .toList
          .flatMap(_.value())
        val eventsSoFar: Seq[E] = events ++ polledEvents
        eventsSoFar.length match {
          case n if n == noOfEventsExpected => eventsSoFar
          case exceeded if exceeded > noOfEventsExpected =>
            throw new Exception(s"Consumed more than $noOfEventsExpected events from $topic")
          case _ => poll(deadline, eventsSoFar)
        }
      } else throw new Exception("Events didn't appear within the timelimit")
    }
    poll(pollTime.fromNow, Nil)

  }
}
