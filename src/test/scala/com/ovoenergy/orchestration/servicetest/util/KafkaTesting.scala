package com.ovoenergy.orchestration.serviceTest.util

import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.serialisation.Decoders._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.typesafe.config.Config
import io.circe.generic.auto._
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
class KafkaTesting(config: Config) {
  val kafkaHosts     = "localhost:29092"
  val zookeeperHosts = "localhost:32181"

  val consumerGroup = Random.nextString(10)

  val triggeredProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[TriggeredV2], kafkaHosts))
  val cancelationRequestedProducer = KafkaProducer(
    KafkaProducerConf(new StringSerializer, avroSerializer[CancellationRequested], kafkaHosts))

  val cancelationRequestedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[CancellationRequested], kafkaHosts, consumerGroup))
  val cancelledConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[Cancelled], kafkaHosts, consumerGroup))
  val commFailedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[Failed], kafkaHosts, consumerGroup))
  val orchestratedEmailConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedEmailV2], kafkaHosts, consumerGroup))
  val smsOrchestratedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedSMS], kafkaHosts, consumerGroup))
  val orchestrationStartedConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestrationStarted], kafkaHosts, consumerGroup))
  val failedCancellationConsumer = KafkaConsumer(
    KafkaConsumerConf(new StringDeserializer, avroDeserializer[FailedCancellation], kafkaHosts, consumerGroup))

  val failedTopic               = config.getString("kafka.topics.failed")
  val triggeredTopic            = config.getString("kafka.topics.triggered.v2")
  val cancellationRequestTopic  = config.getString("kafka.topics.scheduling.cancellationRequest")
  val cancelledTopic            = config.getString("kafka.topics.scheduling.cancelled")
  val failedCancellationTopic   = config.getString("kafka.topics.scheduling.failedCancellation")
  val emailOrchestratedTopic    = config.getString("kafka.topics.orchestrated.email.v2")
  val smsOrchestratedTopic      = config.getString("kafka.topics.orchestrated.sms")
  val orchestrationStartedTopic = config.getString("kafka.topics.orchestration.started")

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

  def setupTopics() {
    import _root_.kafka.admin.AdminUtils
    import _root_.kafka.utils.ZkUtils

    import scala.concurrent.duration._

    val zkUtils = ZkUtils(zookeeperHosts, 30000, 5000, isZkSecurityEnabled = false)

    //Wait until kafka calls are not erroring and the service has created the triggeredTopic
    val timeout    = 10.seconds.fromNow
    var notStarted = true
    while (timeout.hasTimeLeft && notStarted) {
      try {
        notStarted = !AdminUtils.topicExists(zkUtils, triggeredTopic)
      } catch {
        case NonFatal(ex) => Thread.sleep(100)
      }
    }
    if (notStarted) throw new Exception("Services did not start within 10 seconds")

    topics.foreach { topic =>
      if (!AdminUtils.topicExists(zkUtils, failedTopic)) AdminUtils.createTopic(zkUtils, topic, 1, 1)
    }

    failedCancellationConsumer.assign(Seq(new TopicPartition(failedCancellationTopic, 0)).asJava)
    failedCancellationConsumer.poll(5000).records(failedCancellationTopic).asScala.toList
    orchestrationStartedConsumer.assign(Seq(new TopicPartition(orchestrationStartedTopic, 0)).asJava)
    orchestrationStartedConsumer.poll(5000).records(orchestrationStartedTopic).asScala.toList
    commFailedConsumer.assign(Seq(new TopicPartition(failedTopic, 0)).asJava)
    commFailedConsumer.poll(5000).records(failedTopic).asScala.toList
    orchestratedEmailConsumer.assign(Seq(new TopicPartition(emailOrchestratedTopic, 0)).asJava)
    orchestratedEmailConsumer.poll(5000).records(emailOrchestratedTopic).asScala.toList
    smsOrchestratedConsumer.assign(Seq(new TopicPartition(smsOrchestratedTopic, 0)).asJava)
    smsOrchestratedConsumer.poll(5000).records(smsOrchestratedTopic)
    cancelationRequestedConsumer.assign(Seq(new TopicPartition(cancellationRequestTopic, 0)).asJava)
    cancelationRequestedConsumer.poll(5000).records(cancellationRequestTopic).asScala.toList
    cancelledConsumer.assign(Seq(new TopicPartition(cancelledTopic, 0)).asJava)
    cancelledConsumer.poll(5000).records(cancelledTopic).asScala.toList

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
