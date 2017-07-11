package com.ovoenergy.orchestration.kafka

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.ovoenergy.comms.akka.streams.Factory
import com.ovoenergy.comms.akka.streams.Factory.{KafkaConfig, SSLConfig}
import com.ovoenergy.comms.serialisation.Serialisation.avroDeserializer
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings
import com.sksamuel.avro4s.{FromRecord, SchemaFor}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringDeserializer

import scala.reflect.ClassTag

class KafkaSettings(config: Config) {

  private val kafkaGroupId = config.getString("kafka.group.id")

  val schemaRegistryClientSettings = {
    val schemaRegistryEndpoint = config.getString("kafka.aiven.schema_registry.url")
    val schemaRegistryUsername = config.getString("kafka.aiven.schema_registry.username")
    val schemaRegistryPassword = config.getString("kafka.aiven.schema_registry.password")
    SchemaRegistryClientSettings(schemaRegistryEndpoint, schemaRegistryUsername, schemaRegistryPassword)
  }

  def getAivenKafkaConfig(topicKey: String) = {
    val topic        = config.getString(topicKey)
    KafkaConfig(kafkaGroupId, config.getString("kafka.aiven.hosts"), topic, sslConfig)
  }

  def getLegacyKafkaConfig(topicKey: String) = {
    val topic        = config.getString(topicKey)
    KafkaConfig(kafkaGroupId, config.getString("kafka.hosts"), topic, None)
  }

  val sslConfig = {
    if (config.getBoolean("kafka.ssl.enabled")) {
      Some(
        Factory.SSLConfig(
          keystoreLocation = Paths.get(config.getString("kafka.ssl.keystore.location")),
          keystoreType = Factory.StoreType.PKCS12,
          keystorePassword = config.getString("kafka.ssl.keystore.password"),
          keyPassword = config.getString("kafka.ssl.key.password"),
          truststoreLocation = Paths.get(config.getString("kafka.ssl.truststore.location")),
          truststoreType = Factory.StoreType.JKS,
          truststorePassword = config.getString("kafka.ssl.truststore.password")
        ))
    } else None
  }

  def legacyConsumerSettings[T: SchemaFor: FromRecord: ClassTag](config: KafkaConfig)(
      implicit actorSystem: ActorSystem) = {
    ConsumerSettings(actorSystem, new StringDeserializer, avroDeserializer[T])
      .withBootstrapServers(config.hosts)
      .withGroupId(config.groupId)
  }

}
