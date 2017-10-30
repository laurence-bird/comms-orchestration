package servicetest

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.Executors

import cakesolutions.kafka.KafkaConsumer
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import com.whisk.docker.impl.dockerjava.{Docker, DockerJavaExecutorFactory}
import com.whisk.docker._
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.commons.io.input.{Tailer, TailerListenerAdapter}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest._
import servicetest.helpers.DynamoTesting

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

trait DockerIntegrationTest
    extends DockerKit
    with ScalaFutures
    with TestSuite
    with BeforeAndAfterAll
    with Eventually
    with DynamoTesting { self =>

  implicit class RichDockerContainer(val dockerContainer: DockerContainer) {

    /**
      * Adds a log line receiver that writes the container output to a file
      * and a ready checker that tails said file and waits for a line containing a given string
      *
      * @param stringToMatch The container is considered ready when a line containing this string is send to stderr or stdout
      * @param containerName An arbitrary name for the container, used for generating the log file name
      * @param onReady Extra processing to perform when the container is ready, e.g. creating DB tables
      * @return
      */
    def withLogWritingAndReadyChecker(stringToMatch: String,
                                      containerName: String,
                                      onReady: () => Unit = () => ()): DockerContainer = {
      val outputDir = Paths.get("target", "integration-test-logs")
      val outputFile =
        outputDir.resolve(s"${self.getClass.getSimpleName}-$containerName-${LocalDateTime.now().toString}.log")

      val handleLine: String => Unit = (line: String) => {
        val lineWithLineEnding = if (line.endsWith("\n")) line else line + "\n"
        Files.write(outputFile,
                    lineWithLineEnding.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)
      }

      val logLineReceiver = LogLineReceiver(withErr = true, f = handleLine)

      val readyChecker = new DockerReadyChecker {
        override def apply(container: DockerContainerState)(implicit docker: DockerCommandExecutor,
                                                            ec: ExecutionContext): Future[Boolean] = {
          println(s"Waiting for container [$containerName] to become ready. Logs are being streamed to $outputFile.")

          val readyPromise = Promise[Boolean]

          val readyCheckingTailListener = new TailerListenerAdapter {
            var _tailer: Tailer = _

            override def init(tailer: Tailer) = {
              _tailer = tailer
            }

            override def handle(line: String) = {
              if (line.contains(stringToMatch)) {
                onReady()
                println(s"Container [$containerName] is ready")
                readyPromise.trySuccess(true)
                _tailer.stop()
              }
            }
          }

          val tailer = new Tailer(outputFile.toFile, readyCheckingTailListener)
          val thread = new Thread(tailer, s"log tailer for container $containerName")
          thread.start()

          readyPromise.future
        }
      }

      dockerContainer.withLogLineReceiver(logLineReceiver).withReadyChecker(readyChecker)
    }

  }

  override implicit val dockerFactory: DockerFactory = new DockerJavaExecutorFactory(
    new Docker(
      config = DefaultDockerClientConfig.createDefaultConfigBuilder().build(),
      factory = new JerseyDockerCmdExecFactory()
      // increase connection pool size so we can tail the logs of all containers
        .withMaxTotalConnections(100)
        .withMaxPerRouteConnections(20)
    )
  )

  override val StartContainersTimeout = 2.minutes

  override implicit lazy val dockerExecutionContext: ExecutionContext = {
    // using Math.max to prevent unexpected zero length of docker containers
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Math.max(1, dockerContainers.length * 4)))
  }

  val hostIpAddress = {
    import sys.process._
//    "10.200.10.1" // TODO revert me
    "./get_ip_address.sh".!!.trim
  }

  val aivenTopics = Seq(
    "comms.failed.v2",
    "comms.triggered.v3",
    "comms.cancellation.requested.v2",
    "comms.cancelled.v2",
    "comms.failed.cancellation.v2",
    "comms.orchestrated.email.v3",
    "comms.orchestrated.sms.v2",
    "comms.orchestration.started.v2"
  )

  val legacyTopics = aivenTopics ++ Seq(
      "comms.triggered.v2",
      "comms.cancellation.requested"
    )

  // TODO currently no way to set the memory limit on docker containers. Need to make a PR to add support to docker-it-scala. I've checked that the spotify client supports it.

  lazy val legacyZookeeper = DockerContainer("confluentinc/cp-zookeeper:3.1.1", name = Some("legacyZookeeper"))
    .withPorts(32181 -> Some(32181))
    .withEnv(
      "ZOOKEEPER_CLIENT_PORT=32181",
      "ZOOKEEPER_TICK_TIME=2000",
      "KAFKA_HEAP_OPTS=-Xmx256M -Xms128M"
    )
    .withLogWritingAndReadyChecker("binding to port", "legacyZookeeper")

  lazy val legacyKafka = {
    // create each topic with 1 partition and replication factor 1
    val createTopicsString = legacyTopics.map(t => s"$t:1:1").mkString(",")

    val lastTopicName = legacyTopics.last

    DockerContainer("wurstmeister/kafka:0.10.1.0", name = Some("legacyKafka"))
      .withPorts(29092 -> Some(29092))
      .withLinks(ContainerLink(legacyZookeeper, "legacyZookeeper"))
      .withEnv(
        "KAFKA_BROKER_ID=1",
        "KAFKA_ZOOKEEPER_CONNECT=legacyZookeeper:32181",
        "KAFKA_PORT=29092",
        "KAFKA_ADVERTISED_PORT=29092",
        s"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$hostIpAddress:29092",
        "KAFKA_HEAP_OPTS=-Xmx256M -Xms128M",
        s"KAFKA_CREATE_TOPICS=$createTopicsString"
      )
      .withLogWritingAndReadyChecker(s"""Created topic "$lastTopicName"""", "legacyKafka")
  }

  lazy val aivenZookeeper = DockerContainer("confluentinc/cp-zookeeper:3.1.1", name = Some("aivenZookeeper"))
    .withPorts(32182 -> Some(32182))
    .withEnv(
      "ZOOKEEPER_CLIENT_PORT=32182",
      "ZOOKEEPER_TICK_TIME=2000",
      "KAFKA_HEAP_OPTS=-Xmx256M -Xms128M"
    )
    .withLogWritingAndReadyChecker("binding to port", "aivenZookeeper")

  lazy val aivenKafka = {
    // create each topic with 1 partition and replication factor 1
    val createTopicsString = aivenTopics.map(t => s"$t:1:1").mkString(",")

    val lastTopicName = aivenTopics.last

    DockerContainer("wurstmeister/kafka:0.10.2.1", name = Some("aivenKafka"))
      .withPorts(29093 -> Some(29093))
      .withLinks(ContainerLink(aivenZookeeper, "aivenZookeeper"))
      .withEnv(
        "KAFKA_BROKER_ID=2",
        "KAFKA_ZOOKEEPER_CONNECT=aivenZookeeper:32182",
        "KAFKA_PORT=29093",
        "KAFKA_ADVERTISED_PORT=29093",
        s"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$hostIpAddress:29093",
        "KAFKA_HEAP_OPTS=-Xmx256M -Xms128M",
        s"KAFKA_CREATE_TOPICS=$createTopicsString"
      )
      .withLogWritingAndReadyChecker(s"""Created topic "$lastTopicName"""", "aivenKafka")
  }

  lazy val schemaRegistry = DockerContainer("confluentinc/cp-schema-registry:3.2.2", name = Some("schema-registry"))
    .withPorts(8081 -> Some(8081))
    .withLinks(
      ContainerLink(aivenZookeeper, "aivenZookeeper"),
      ContainerLink(aivenKafka, "aivenKafka")
    )
    .withEnv(
      "SCHEMA_REGISTRY_HOST_NAME=schema-registry",
      "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=aivenZookeeper:32182",
      s"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://$hostIpAddress:29093"
    )
    .withLogWritingAndReadyChecker("Server started, listening for requests", "schema-registry")

  lazy val profiles = DockerContainer("jamesdbloom/mockserver:latest", name = Some("profiles"))
    .withPorts(1080 -> Some(1080))
    .withLogWritingAndReadyChecker("MockServer proxy started", "profiles")

  lazy val fakes3 = DockerContainer("lphoward/fake-s3:latest", name = Some("fakes3"))
    .withPorts(4569 -> Some(4569))
    .withLogWritingAndReadyChecker("WEBrick::HTTPServer#start", "fakes3")

  lazy val fakes3ssl = DockerContainer("cbachich/ssl-proxy:latest", name = Some("fakes3ssl"))
    .withPorts(443 -> Some(443))
    .withLinks(ContainerLink(fakes3, "proxyapp"))
    .withEnv(
      "PORT=443",
      "TARGET_PORT=4569"
    )
    .withLogWritingAndReadyChecker("Starting Proxy: 443", "fakes3ssl")

  lazy val dynamodb = DockerContainer("forty8bit/dynamodb-local:latest", name = Some("dynamodb"))
    .withPorts(8000 -> Some(8000))
    .withCommand("-sharedDb")
//    .withCommand("java",
//                 "-Xmx256M",
//                 "-Xms128M",
//                 "-Djava.library.path=./DynamoDBLocal_lib",
//                 "-jar",
//                 "DynamoDBLocal.jar",
//                 "-sharedDb") // TODO we need to override the entrypoint, not pass a command. This will need a PR against docker-it-scala.
    .withLogWritingAndReadyChecker("Initializing DynamoDB Local", "dynamodb", onReady = () => {
      println("Creating Dynamo table")
      createTable()
    })

  lazy val orchestration = {
    val awsAccountId = sys.env.getOrElse(
      "AWS_ACCOUNT_ID",
      sys.error("Environment variable AWS_ACCOUNT_ID must be set in order to run the integration tests"))

    val envVars = List(
      sys.env.get("AWS_ACCESS_KEY_ID").map(envVar => s"AWS_ACCESS_KEY_ID=$envVar"),
      sys.env.get("AWS_ACCOUNT_ID").map(envVar => s"AWS_ACCOUNT_ID=$envVar"),
      sys.env.get("AWS_SECRET_ACCESS_KEY").map(envVar => s"AWS_SECRET_ACCESS_KEY=$envVar"),
      Some("ENV=LOCAL"),
      Some("RUNNING_IN_DOCKER=true"),
      Some("PROFILE_SERVICE_API_KEY=someApiKey"),
      Some("PROFILE_SERVICE_HOST=http://profiles:1080"),
      Some("POLL_FOR_EXPIRED_INTERVAL=5 seconds"),
      Some("KAFKA_HOSTS=legacyKafka:29092"),
      Some("KAFKA_HOSTS_AIVEN=aivenKafka:29093"),
      Some("LOCAL_DYNAMO=http://dynamodb:8000"),
      Some("SCHEMA_REGISTRY_URL=http://schema-registry:8081")
    ).flatten

    DockerContainer(s"$awsAccountId.dkr.ecr.eu-west-1.amazonaws.com/orchestration:0.1-SNAPSHOT",
                    name = Some("orchestration"))
      .withLinks(
        ContainerLink(profiles, "profiles"),
        ContainerLink(legacyZookeeper, "legacyZookeeper"),
        ContainerLink(legacyKafka, "legacyKafka"),
        ContainerLink(aivenZookeeper, "aivenZookeeper"),
        ContainerLink(aivenKafka, "aivenKafka"),
        ContainerLink(schemaRegistry, "schema-registry"),
        ContainerLink(dynamodb, "dynamodb"),
        ContainerLink(fakes3ssl, "ovo-comms-templates.s3-eu-west-1.amazonaws.com")
      )
      .withEnv(envVars: _*)
      .withVolumes(List(VolumeMapping(host = s"${sys.env("HOME")}/.aws", container = "/sbin/.aws"))) // share AWS creds so that credstash works
      .withLogWritingAndReadyChecker("Orchestration started", "orchestration")
  }

  override def dockerContainers =
    List(legacyZookeeper,
         legacyKafka,
         aivenZookeeper,
         aivenKafka,
         schemaRegistry,
         fakes3,
         fakes3ssl,
         dynamodb,
         profiles,
         orchestration)

  def checkCanConsumeFromKafkaTopic(topic: String, bootstrapServers: String, description: String) {
    println(s"Checking we can consume from topic $topic on $description Kafka")
    import cakesolutions.kafka.KafkaConsumer._
    import scala.collection.JavaConverters._
    val consumer = KafkaConsumer(
      Conf[String, String](Map("bootstrap.servers" -> bootstrapServers, "group.id" -> UUID.randomUUID().toString),
                           new StringDeserializer,
                           new StringDeserializer))
    consumer.assign(List(new TopicPartition(topic, 0)).asJava)
    eventually(PatienceConfiguration.Timeout(Span(20, Seconds))) {
      consumer.poll(200)
    }
    consumer.close()
    println("Yes we can!")
  }

  abstract override def beforeAll(): Unit = {
    super.beforeAll()

    import scala.collection.JavaConverters._
    val logDir = Paths.get("target", "integration-test-logs")
    if (Files.exists(logDir)) {
      Files.list(logDir).iterator().asScala.foreach { f =>
        // delete any leftover docker log files from a previous run of this test class
        if (f.toFile.getName.contains(self.getClass.getSimpleName)) {
          Files.delete(f)
        }
      }
    } else
      Files.createDirectories(logDir)

    println(
      "Starting a whole bunch of Docker containers. This could take a few minutes, but I promise it'll be worth the wait!")
    startAllOrFail()

    legacyTopics.foreach(t => checkCanConsumeFromKafkaTopic(t, "localhost:29092", "legacy"))
    aivenTopics.foreach(t => checkCanConsumeFromKafkaTopic(t, "localhost:29093", "Aiven"))
  }

  abstract override def afterAll(): Unit = {
    stopAllQuietly()

    super.afterAll()
  }

}
