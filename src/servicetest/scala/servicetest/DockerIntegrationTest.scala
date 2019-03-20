package servicetest

import java.net.NetworkInterface
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.Executors

import cakesolutions.kafka.KafkaConsumer
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import com.whisk.docker.impl.dockerjava.{Docker, DockerJavaExecutorFactory}
import com.whisk.docker._
import org.apache.commons.io.input.{Tailer, TailerListenerAdapter}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest._
import servicetest.helpers.DynamoTesting

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

import com.ovoenergy.comms.orchestrator.BuildInfo

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

  override val StartContainersTimeout            = 15.minutes
  override val PullImagesTimeout: FiniteDuration = scaled(30.minutes)

  override implicit lazy val dockerExecutionContext: ExecutionContext = {
    // using Math.max to prevent unexpected zero length of docker containers
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Math.max(1, dockerContainers.length * 4)))
  }

  import scala.collection.JavaConverters._

  private val hostIpAddress = NetworkInterface.getNetworkInterfaces.asScala
    .filter(x => x.isUp && !x.isLoopback)
    .flatMap(_.getInterfaceAddresses.asScala)
    .map(_.getAddress)
    .find(_.isSiteLocalAddress)
    .fold(throw new RuntimeException("Local ip address not found"))(_.getHostAddress)

  val aivenTopics = Seq(
    "comms.feedback",
    "comms.failed.v3",
    "comms.triggered.v4",
    "comms.orchestrated.email.v4",
    "comms.orchestrated.sms.v3",
    "comms.orchestration.started.v3"
  )

  // TODO currently no way to set the memory limit on docker containers. Need to make a PR to add support to docker-it-scala. I've checked that the spotify client supports it.

  lazy val aivenZookeeper = DockerContainer("confluentinc/cp-zookeeper:3.3.1", name = Some("zookeeper"))
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

    DockerContainer("confluentinc/cp-kafka:3.3.1", name = Some("kafka"))
      .withPorts(29093 -> Some(29093))
      .withLinks(ContainerLink(aivenZookeeper, "zookeeper"))
      .withEnv(
        s"KAFKA_ZOOKEEPER_CONNECT=zookeeper:32182",
        "KAFKA_BROKER_ID=1",
        s"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$hostIpAddress:29093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
      )
      .withLogWritingAndReadyChecker(s"""started (kafka.server.KafkaServer)""", "aivenKafka")
  }

  lazy val schemaRegistry = DockerContainer("confluentinc/cp-schema-registry:3.3.1", name = Some("schema-registry"))
    .withPorts(8081 -> Some(8081))
    .withLinks(
      ContainerLink(aivenZookeeper, "zookeeper")
    )
    .withEnv(
      "SCHEMA_REGISTRY_HOST_NAME=schema-registry",
      "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:32182",
      s"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://$hostIpAddress:29093"
    )
    .withLogWritingAndReadyChecker("Server started, listening for requests", "schema-registry")

  lazy val profiles = DockerContainer("jamesdbloom/mockserver:mockserver-3.12", name = Some("profiles"))
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
      createTemplateSummaryTable()
      createEventDeduplicationTable()
    })

  lazy val orchestration = {
    
    val awsAccountId = "852955754882"

    val envVars = List(
      Some("JAVA_OPTS=-Dlogback.configurationFile=logback-local.xml -Dcom.amazonaws.sdk.disableCertChecking=true"),
      Some(s"TEMPLATE_SUMMARY_TABLE=$templateSummaryTableName"),
      Some(s"SCHEDULER_TABLE=$tableName"),
      Some(s"DEDUPLICATION_TABLE=$eventDeduplicationTableName"),
      Some("PROFILES_ENDPOINT=http://profiles:1080"),
      Some("PROFILES_API_KEY=someApiKey"),
      Some("DYNAMO_DB_ENDPOINT=http://dynamodb:8000"),
      Some("KAFKA_BOOTSTRAP_SERVERS=aivenKafka:29093"),
      Some("KAFKA_CONSUMER_GROUP_ID=orchestrator-test"),
      Some("SCHEMA_REGISTRY_ENDPOINT=http://schema-registry:8081")
    ).flatten

    DockerContainer(s"$awsAccountId.dkr.ecr.eu-west-1.amazonaws.com/orchestrator:${BuildInfo.version}",
                    name = Some("orchestration"))
      .withLinks(
        ContainerLink(profiles, "profiles"),
        ContainerLink(aivenZookeeper, "aivenZookeeper"),
        ContainerLink(aivenKafka, "aivenKafka"),
        ContainerLink(schemaRegistry, "schema-registry"),
        ContainerLink(dynamodb, "dynamodb"),
        ContainerLink(fakes3ssl, "ovo-comms-templates.s3.eu-west-1.amazonaws.com") 
      )
      .withEnv(envVars: _*)
      .withVolumes(List(VolumeMapping(host = s"${sys.env("HOME")}/.aws", container = "/sbin/.aws"))) // share AWS creds so that credstash works
      .withLogWritingAndReadyChecker("Orchestration started", "orchestration")
  }

  override def dockerContainers =
    List(aivenZookeeper, aivenKafka, schemaRegistry, fakes3, fakes3ssl, dynamodb, profiles, orchestration)

  def createTopics(topics: Iterable[String], bootstrapServers: String) {
    println(s"Creating kafka topics")
    import scala.collection.JavaConverters._

    val adminClient =
      AdminClient.create(Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava)
    try {
      val r = adminClient.createTopics(topics.map(t => new NewTopic(t, 1, 1)).asJavaCollection)
      r.all().get()
    } catch {
      case e: java.util.concurrent.ExecutionException => ()
    } finally {
      adminClient.close()
    }
  }

  def checkCanConsumeFromKafkaTopic(topic: String, bootstrapServers: String) {
    println(s"Checking we can consume from topic $topic")
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
    val bootstrapServers = "localhost:29093"
    createTopics(aivenTopics, bootstrapServers)
    aivenTopics.foreach(t => checkCanConsumeFromKafkaTopic(t, bootstrapServers))
  }

  abstract override def afterAll(): Unit = {
    stopAllQuietly()
    super.afterAll()
  }
}
