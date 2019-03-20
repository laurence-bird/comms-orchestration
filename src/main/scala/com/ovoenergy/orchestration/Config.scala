package com.ovoenergy.orchestration

import scala.concurrent.duration._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import ciris._
import ciris.cats.effect._
import ciris.credstash.credstashF
import ciris.aiven.kafka.aivenKafkaSetup

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._

import org.http4s.Uri

import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings

import com.ovoenergy.comms.model._
import com.ovoenergy.comms.model.email._
import com.ovoenergy.comms.model.sms._
import com.ovoenergy.comms.model.print._
import com.ovoenergy.comms.deduplication.{Config => DeduplicationConfig}

case class Config(
    templateSummaryTable: String,
    schedulingTable: String,
    profilesEndpoint: Uri,
    profilesApiKey: String,
    kafka: Config.Kafka,
    eventDeduplication: DeduplicationConfig[String],
    communicationDeduplication: DeduplicationConfig[String],
    schedulingExpiration: FiniteDuration = 5.minutes,
    loadPendingDelay: FiniteDuration = 5.minutes,
    pollForExpiredDelay: FiniteDuration = 5.minutes,
    pollForExpiredInterval: FiniteDuration = 10.minutes,
    dynamoDbEndpoint: Option[String] = None,
    triggeredTopics: NonEmptyList[Config.Topic[TriggeredV4]] = NonEmptyList.of(
      Config.Topic[TriggeredV4]("comms.triggered.v4"),
      Config.Topic[TriggeredV4]("comms_triggered_p0_v4")
    ),
    orchestrationStartedTopic: Config.Topic[OrchestrationStartedV3] =
      Config.Topic[OrchestrationStartedV3]("comms.orchestration.started.v3"),
    feedbackTopic: Config.Topic[Feedback] = Config.Topic[Feedback]("comms.feedback"),
    failedTopic: Config.Topic[FailedV3] = Config.Topic[FailedV3]("comms.failed.v3"),
    orchestratedEmailTopic: Config.Topic[OrchestratedEmailV4] =
      Config.Topic[OrchestratedEmailV4]("comms.orchestrated.email.v4"),
    orchestratedSmsTopic: Config.Topic[OrchestratedSMSV3] = Config.Topic[OrchestratedSMSV3]("comms.orchestrated.sms.v3"),
    orchestratedPrintTopic: Config.Topic[OrchestratedPrintV2] =
      Config.Topic[OrchestratedPrintV2]("comms.orchestrated.print.v2")
) {

  def amazonDynamoDbClientBuilder: AmazonDynamoDBAsyncClientBuilder = {
    val initial = AmazonDynamoDBAsyncClientBuilder
      .standard()

    dynamoDbEndpoint.fold(initial) { endpoint =>
      initial.withEndpointConfiguration(new EndpointConfiguration(endpoint, "eu-west-1"))
    }
  }

}

object Config {

  implicit val uriConfigDecoder: ConfigDecoder[String, Uri] =
    ConfigDecoder.fromTry("uri")(str => Uri.fromString(str).toTry)

  sealed trait Env {
    def toStringLowerCase: String = toString.toLowerCase
  }

  object Env {
    case object Uat extends Env
    case object Prd extends Env

    def fromString(str: String): Either[String, Env] = str.toUpperCase match {
      case "UAT" => Right(Uat)
      case "PRD" => Right(Prd)
      case other => Left(s"$other is not a known environment")
    }

    implicit val envConfigDecoder: ConfigDecoder[String, Env] =
      ConfigDecoder.fromTry("env")(str => Env.fromString(str).leftMap(err => new IllegalArgumentException(err)).toTry)
  }

  case class Kafka(
      consumer: Map[String, String],
      producer: Map[String, String],
      schemaRegistry: SchemaRegistryClientSettings
  )

  object Kafka {

    def loadFromUnknownEnvironment[F[_]: Sync]: ConfigResult[F, Config.Kafka] = {
      val schemaRegistryConfig =
        loadConfig(
          envF[F, String]("SCHEMA_REGISTRY_ENDPOINT")
        ) { endpoint =>
          SchemaRegistryClientSettings(endpoint)
        }

      loadConfig(
        envF[F, String]("KAFKA_BOOTSTRAP_SERVERS"),
        envF[F, Option[String]]("KAFKA_CONSUMER_GROUP_ID"),
        schemaRegistryConfig
      ) { (kafkaBootstrapServers, kafkaConsumerGroupIdOpt, schemaRegistrySettings) =>
        val consumerNativeSettings = Map(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrapServers,
          ConsumerConfig.GROUP_ID_CONFIG          -> kafkaConsumerGroupIdOpt.getOrElse("comms-orchestrator"),
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        )

        val producerNativeSettings = Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrapServers,
          // TODO Enable idempotency back when https://issues.apache.org/jira/browse/KAFKA-6817 is solved
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> "false",
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1",
          ProducerConfig.RETRIES_CONFIG                        -> "5",
          ProducerConfig.ACKS_CONFIG                           -> "all"
        )

        Config.Kafka(
          consumerNativeSettings,
          producerNativeSettings,
          schemaRegistrySettings,
        )
      }
    }

    def loadFromKnownEnvironment[F[_]: Sync](env: Env): ConfigResult[F, Config.Kafka] = {

      val environment = env.toStringLowerCase

      def schemaRegistryConfig(environment: String) =
        loadConfig(
          credstashF[F, String]()(s"$environment.aiven.schema_registry.url"),
          credstashF[F, String]()(s"$environment.aiven.schema_registry.username"),
          credstashF[F, String]()(s"$environment.aiven.schema_registry.password")
        ) { (endpoint, username, password) =>
          SchemaRegistryClientSettings(
            endpoint,
            username,
            password
          )
        }

      loadConfig(
        aivenKafkaSetup(
          clientPrivateKey = credstashF()(s"$environment.kafka.client_private_key"),
          clientCertificate = credstashF()(s"$environment.kafka.client_certificate"),
          serviceCertificate = credstashF()(s"$environment.kafka.service_certificate")
        ),
        credstashF[F, String]()(s"$environment.aiven.kafka_hosts"),
        envF[F, Option[String]]("KAFKA_CONSUMER_GROUP_ID"),
        schemaRegistryConfig(environment),
      ) { (aivenKafkaSetup, bootstrapServers, kafkaConsumerGroupId, schemaRegistrySettings) =>
        {

          val consumerNativeSettings = aivenKafkaSetup.setProperties(
            Map(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
              ConsumerConfig.GROUP_ID_CONFIG          -> kafkaConsumerGroupId.getOrElse("comms-orchestrator"),
              ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
            )) { (acc, k, v) =>
            acc + (k -> v)
          }

          val producerNativeSettings = aivenKafkaSetup.setProperties(
            Map(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
              // TODO Enable idempotency back when https://issues.apache.org/jira/browse/KAFKA-6817 is solved
              ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> "false",
              ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1",
              ProducerConfig.RETRIES_CONFIG                        -> "5",
              ProducerConfig.ACKS_CONFIG                           -> "all"
            )) { (acc, k, v) =>
            acc + (k -> v)
          }

          Config.Kafka(
            consumerNativeSettings,
            producerNativeSettings,
            schemaRegistrySettings
          )
        }
      }
    }

  }

  final case class Topic[A](value: String) extends AnyVal

  implicit def topicConfigDecoder[A]: ConfigDecoder[String, Topic[A]] =
    ConfigDecoder.catchNonFatal("topic")(Topic[A](_))

  def load[F[_]: Sync]: ConfigResult[F, Config] = {
    withValues(
      envF[F, Option[Env]]("ENV")
    ) { (environmentOpt) =>
      environmentOpt.fold(loadFromUnknownEnvironment)(loadFromKnownEnvironment)
    }
  }

  def loadFromUnknownEnvironment[F[_]: Sync] =
    loadConfig(
      envF[F, String]("TEMPLATE_SUMMARY_TABLE"),
      envF[F, String]("SCHEDULER_TABLE"),
      envF[F, String]("DEDUPLICATION_TABLE"),
      envF[F, Option[String]]("DYNAMO_DB_ENDPOINT"),
      envF[F, Uri]("PROFILES_ENDPOINT"),
      envF[F, String]("PROFILES_API_KEY"),
      Kafka.loadFromUnknownEnvironment[F],
    ) {
      (templateSummaryTable,
       schedulerTable,
       eventDeduplicationTable,
       dynamoDbEndpoint,
       profilesEndpoint,
       profilesApiKey,
       kafka) =>
        Config(
          templateSummaryTable = templateSummaryTable,
          schedulingTable = schedulerTable,
          dynamoDbEndpoint = dynamoDbEndpoint,
          profilesEndpoint = profilesEndpoint,
          profilesApiKey = profilesApiKey,
          kafka = kafka,
          eventDeduplication = DeduplicationConfig(
            tableName = DeduplicationConfig.TableName(eventDeduplicationTable),
            processorId = "orchestrator",
            maxProcessingTime = 5.seconds,
            ttl = 35.days,
          ),
          communicationDeduplication = DeduplicationConfig(
            tableName = DeduplicationConfig.TableName(eventDeduplicationTable),
            processorId = "platform",
            maxProcessingTime = 5.seconds,
            ttl = 15.days,
          )
        )
    }

  def loadFromKnownEnvironment[F[_]: Sync](env: Env): ConfigResult[F, Config] =
    loadConfig(
      envF[F, String]("TEMPLATE_SUMMARY_TABLE"),
      envF[F, String]("SCHEDULER_TABLE"),
      envF[F, String]("DEDUPLICATION_TABLE"),
      envF[F, Uri]("PROFILES_ENDPOINT"),
      credstashF[F, String]()(s"${env.toStringLowerCase}.orchestration.profilesApiKey"),
      Kafka.loadFromKnownEnvironment(env)
    ) { (templateSummaryTable, schedulerTable, eventDeduplicationTable, profilesEndpoint, profilesApiKey, kafka) =>
      Config(
        templateSummaryTable = templateSummaryTable,
        schedulingTable = schedulerTable,
        profilesEndpoint = profilesEndpoint,
        profilesApiKey = profilesApiKey,
        kafka = kafka,
        eventDeduplication = DeduplicationConfig(
          tableName = DeduplicationConfig.TableName(eventDeduplicationTable),
          processorId = "orchestrator",
          maxProcessingTime = 5.seconds,
          ttl = 35.days,
        ),
        communicationDeduplication = DeduplicationConfig(
          tableName = DeduplicationConfig.TableName(eventDeduplicationTable),
          processorId = "platform",
          maxProcessingTime = 5.seconds,
          ttl = 15.days,
        )
      )
    }

}
