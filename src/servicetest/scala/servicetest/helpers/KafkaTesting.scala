package servicetest.helpers

import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email._
import com.ovoenergy.comms.model.sms._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Codecs._
import com.ovoenergy.kafka.serialization.avro.{Authentication, SchemaRegistryClientSettings}
import com.ovoenergy.kafka.serialization.avro4s.{avroBinarySchemaIdDeserializer, avroBinarySchemaIdSerializer}
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer => ApacheKafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.ovoenergy.kafka.serialization.avro4s.{avroBinarySchemaIdDeserializer, avroBinarySchemaIdSerializer}
import com.ovoenergy.comms.serialisation._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration, _}
import scala.reflect.ClassTag
import scala.util.Random

class KafkaTesting(config: Config) {
  val aivenKafkaHosts     = "localhost:29093"
  val aivenZookeeperHosts = "localhost:32182"
  val kafkaHosts          = "localhost:29092"
  val zookeeperHosts      = "localhost:32181"

  val consumerGroup = Random.nextString(10)

  val failedTopic               = config.getString("kafka.topics.failed.v2")
  val triggeredTopic            = config.getString("kafka.topics.triggered.v3")
  val cancellationRequestTopic  = config.getString("kafka.topics.scheduling.cancellationRequest.v2")
  val cancelledTopic            = config.getString("kafka.topics.scheduling.cancelled.v2")
  val failedCancellationTopic   = config.getString("kafka.topics.scheduling.failedCancellation.v2")
  val orchestratedEmailTopic    = config.getString("kafka.topics.orchestrated.email.v3")
  val orchestratedSmsTopic      = config.getString("kafka.topics.orchestrated.sms.v2")
  val orchestrationStartedTopic = config.getString("kafka.topics.orchestration.started.v2")

  val triggeredV2Topic             = config.getString("kafka.topics.triggered.v2")
  val cancellationRequestedV1Topic = config.getString("kafka.topics.scheduling.cancellationRequest.v1")

  val aivenTopics = Seq(
    failedTopic,
    triggeredTopic,
    cancellationRequestTopic,
    cancelledTopic,
    failedCancellationTopic,
    orchestratedEmailTopic,
    orchestratedSmsTopic,
    orchestrationStartedTopic
  )

  val legacyTopics = aivenTopics ++ Seq(
      triggeredV2Topic,
      cancellationRequestedV1Topic
    )

  val schemaRegistrySettings = SchemaRegistryClientSettings("http://localhost:8081", Authentication.None, 100, 1)

  private val allKafkaProducers = ArrayBuffer.empty[KafkaProducer[_, _]]
  private val allKafkaConsumers = ArrayBuffer.empty[ApacheKafkaConsumer[_, _]]

  private def kafkaProducer[A: SchemaFor: ToRecord] = {
    val producer = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[A], kafkaHosts))
    allKafkaProducers.append(producer)
    producer
  }

  private def kafkaConsumer[A: SchemaFor: FromRecord: ClassTag](topic: String) = {
    val consumer = KafkaConsumer(
      KafkaConsumerConf(new StringDeserializer, avroDeserializer[A], kafkaHosts, consumerGroup))
    allKafkaConsumers.append(consumer)
    consumer.assign(Seq(new TopicPartition(topic, 0)).asJava)
    consumer
  }

  private def aivenProducer[T: SchemaFor: ToRecord](topic: String): KafkaProducer[String, T] = {
    val producer = KafkaProducer(
      KafkaProducerConf(new StringSerializer,
                        avroBinarySchemaRegistrySerializer[T](schemaRegistrySettings, topic),
                        aivenKafkaHosts)
    )
    producer
  }

  private def aivenConsumer[T: SchemaFor: FromRecord: ClassTag](topic: String) = {
    val consumer = KafkaConsumer(
      KafkaConsumerConf(new StringDeserializer,
                        avroBinarySchemaRegistryDeserializer[T](schemaRegistrySettings, topic),
                        aivenKafkaHosts,
                        consumerGroup)
    )
    consumer.assign(Seq(new TopicPartition(topic, 0)).asJava)
    consumer
  }

  /*
  Note: The consumers and producers are lazy so we don't create and assign them
  until after we've tested the topics can be consumed from.

  BUT! You have to assign the consumer to the topic before you send any events to it,
  hence the initKafkaConsumers() method below.
   */
  lazy val legacyTriggeredProducer             = kafkaProducer[TriggeredV2]
  lazy val triggeredProducer                   = kafkaProducer[TriggeredV3]
  lazy val legacyCancellationRequestedProducer = kafkaProducer[CancellationRequested]
  lazy val cancellationRequestedProducer       = kafkaProducer[CancellationRequestedV2]

  lazy val aivenCancellationRequestedProducer = aivenProducer[CancellationRequestedV2](cancellationRequestTopic)
  lazy val aivenTriggeredProducer             = aivenProducer[TriggeredV3](triggeredTopic)
  lazy val cancellationRequestedConsumer      = aivenConsumer[CancellationRequestedV2](cancellationRequestTopic)
  lazy val cancelledConsumer                  = aivenConsumer[CancelledV2](cancelledTopic)
  lazy val commFailedConsumer                 = aivenConsumer[FailedV2](failedTopic)
  lazy val orchestratedEmailConsumer          = aivenConsumer[OrchestratedEmailV3](orchestratedEmailTopic)
  lazy val smsOrchestratedConsumer            = aivenConsumer[OrchestratedSMSV2](orchestratedSmsTopic)
  lazy val orchestrationStartedConsumer       = aivenConsumer[OrchestrationStartedV2](orchestrationStartedTopic)
  lazy val failedCancellationConsumer         = aivenConsumer[FailedCancellationV2](failedCancellationTopic)

  def initKafkaConsumers(): Unit = {
    Seq(
      cancellationRequestedConsumer,
      cancelledConsumer,
      commFailedConsumer,
      orchestratedEmailConsumer,
      smsOrchestratedConsumer,
      orchestrationStartedConsumer,
      failedCancellationConsumer
    ).foreach { consumer =>
      consumer.poll(100)
    }
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
      } else throw new Exception(s"Events didn't appear within the timelimit (got ${events.size} events)")
    }

    poll(pollTime.fromNow, Nil)
  }

  def shutdownAllKafkaProducers(): Unit = allKafkaProducers.foreach(_.close())

  def shutdownAllKafkaConsumers(): Unit = allKafkaConsumers.foreach(_.close())

}