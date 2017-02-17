package com.ovoenergy.orchestration

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, AttributeValue, PutItemRequest}
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.ErrorCode.{InvalidProfile, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Decoders._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import util.{LocalDynamoDB, TestUtil}
import io.circe.generic.auto._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.mockserver.client.server.MockServerClient
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}
import util.LocalDynamoDB.SecondaryIndexData

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global

class ServiceTestIT extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with IntegrationPatience {

  object DockerComposeTag extends Tag("DockerComposeTag")

  override def beforeAll() = {
    createTable()
    setupTopics()
  }

  override def afterAll() = {
    dynamoClient.deleteTable(tableName)
  }

  val mockServerClient = new MockServerClient("localhost", 1080)

  val kafkaHosts = "localhost:29092"
  val zookeeperHosts = "localhost:32181"

  val config =
    ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))

  val consumerGroup                 = Random.nextString(10)
  val triggeredProducer             = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[TriggeredV2], kafkaHosts))
  val cancelationRequestedProducer  = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[CancellationRequested], kafkaHosts))
  val cancelationRequestedConsumer  = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[CancellationRequested], kafkaHosts, consumerGroup))
  val cancelledConsumer             = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[Cancelled], kafkaHosts, consumerGroup))
  val commFailedConsumer            = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[Failed], kafkaHosts, consumerGroup))
  val emailOrchestratedConsumer     = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedEmailV2], kafkaHosts, consumerGroup))
  val failedCancellationConsumer    = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[FailedCancellation], kafkaHosts, consumerGroup))

  val dynamoUrl = "http://localhost:8000"
  val dynamoClient = LocalDynamoDB.client(dynamoUrl)
  val tableName = "scheduling"

  val failedTopic                 = config.getString("kafka.topics.failed")
  val triggeredTopic              = config.getString("kafka.topics.triggered.v2")
  val cancellationRequestTopic    = config.getString("kafka.topics.scheduling.cancellationRequest")
  val cancelledTopic              = config.getString("kafka.topics.scheduling.cancelled")
  val failedCancellationTopic     = config.getString("kafka.topics.scheduling.failedCancellation")
  val emailOrchestratedTopic      = config.getString("kafka.topics.orchestrated.email.v2")

  behavior of "Service Testing"

  it should "orchestrate emails request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      _ => expectOrchestrationEvents(10000.millisecond, 1)
    }
  }

  it should "generate unique internalTraceTokens" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      _ =>
        val firstEvent = expectOrchestrationEvents(noOfEventsExpected = 1)
        val secondFuture = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
        whenReady(secondFuture) {
          _ =>
            val secondEvent = expectOrchestrationEvents(noOfEventsExpected = 1)
            secondEvent.head.internalMetadata.internalTraceToken should not equal firstEvent.head.internalMetadata.internalTraceToken
        }
    }
  }

  it should "orchestrate emails requested to be sent in the future" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val triggered = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(5).toString))
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    whenReady(future) {
      _ =>
        val orchestratedEmails = emailOrchestratedConsumer.poll(4500).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails shouldBe empty
        expectOrchestrationEvents(noOfEventsExpected = 1)
    }
  }

  it should "orchestrate multiple emails" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()

    var futures = new mutable.ListBuffer[Future[_]]
    (1 to 10).foreach(counter => {
      val triggered = TestUtil.triggered.copy(metadata = TestUtil.metadata.copy(traceToken = counter.toString))
      futures += triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    })

    futures.foreach(future => Await.ready(future, 1.seconds))

    val deadline = 15.seconds
    val orchestratedEmails = expectOrchestrationEvents(pollTime = deadline, noOfEventsExpected = 10, shouldCheckTraceToken = false)
    orchestratedEmails.map(_.metadata.traceToken) should contain allOf("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  def expectOrchestrationEvents(pollTime: FiniteDuration = 20000.millisecond, noOfEventsExpected: Int, shouldCheckTraceToken: Boolean = true) = {
    def poll(deadline: Deadline, emails: Seq[OrchestratedEmailV2]): Seq[OrchestratedEmailV2]= {
      if(deadline.hasTimeLeft){
        val orchestratedEmails = emailOrchestratedConsumer
          .poll(100)
          .records(emailOrchestratedTopic)
          .asScala
          .toList
          .flatMap(_.value())
        val emailsSoFar = orchestratedEmails ++ emails
        emailsSoFar.length match {
          case n if n == noOfEventsExpected => emailsSoFar
          case exceeded if exceeded > noOfEventsExpected => fail(s"Consumed more than $noOfEventsExpected orchestrated email event")
          case _ => poll(deadline, emailsSoFar)
        }
      } else fail("Email was not orchestrated within time limit")
    }
    val orchestratedEmails = poll(pollTime.fromNow, Nil)
    orchestratedEmails.map{ orchestratedEmail =>
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe model.CustomerProfile("Gary", "Philpott")
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if(shouldCheckTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
      orchestratedEmail
    }
  }

 it should "raise failure for customers with insufficient details to orchestrate emails for" taggedAs DockerComposeTag in {
   createInvalidCustomerProfileResponse()

   val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
   whenReady(future) {
     _ =>
       val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
       failures.size shouldBe 1
       failures.foreach(record => {
         val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
         failure.reason should include("Customer has no usable email address")
         failure.reason should include("Customer has no last name")
         failure.reason should include("Customer has no first name")
         failure.errorCode shouldBe InvalidProfile
         failure.metadata.traceToken shouldBe TestUtil.traceToken
       })
   }
 }

 it should "raise failure when customer profiler fails" taggedAs DockerComposeTag in {
   createBadCustomerProfileResponse()

   val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
   whenReady(future) {
     _ =>
       val failures = commFailedConsumer.poll(30000).records(failedTopic).asScala.toList
       failures.size shouldBe 1
       failures.foreach(record => {
         val failure = record.value().getOrElse(fail("No record for ${record.key()}"))
         failure.reason should include("Error response (500) from profile service: Some error")
         failure.metadata.traceToken shouldBe TestUtil.traceToken
         failure.errorCode shouldBe ProfileRetrievalFailed
       })
   }
 }

  it should "retry if the profile service returns an error response" taggedAs DockerComposeTag in {
    createFlakyCustomerProfileResponse()

    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      _ =>
        val orchestratedEmails = emailOrchestratedConsumer.poll(30000).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails.size shouldBe 1
    }
  }

  it should "pick up expired events via polling and orchestrate them" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()

    import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence._
    val schedulesTable = Table[Schedule](tableName)
    val triggered = TestUtil.triggered

    Scanamo.exec(dynamoClient)(schedulesTable.put(
      Schedule(
        "testSchedule",
        triggered,
        Instant.now().minusSeconds(600),
        ScheduleStatus.Orchestrating,
        Nil,
        Instant.now().minusSeconds(60),
        triggered.metadata.customerId,
        triggered.metadata.commManifest.name
      )
    ))

    expectOrchestrationEvents(noOfEventsExpected = 1)
  }

  it should "deschedule comms and generate cancelled events" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val triggered1 = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(20).toString))
    val triggered2 = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(21).toString), metadata = triggered1.metadata.copy(traceToken = "testTrace123"))

    // 2 trigger events for the same customer and comm
    val triggeredEvents = Seq(
      triggered1,
      triggered2
    )
    val genericMetadata = GenericMetadata(triggeredEvents.head.metadata.createdAt, triggered1.metadata.eventId, triggered1.metadata.traceToken, triggered1.metadata.source, false)
    val cancellationRequested: CancellationRequested = CancellationRequested(genericMetadata, triggered1.metadata.commManifest.name, triggered1.metadata.customerId)

    val triggeredFuture = Future.sequence(triggeredEvents.map(tr => triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, tr))))
    Thread.sleep(100)
    val cancelledFuture = triggeredFuture.flatMap(t => cancelationRequestedProducer.send(new ProducerRecord[String, CancellationRequested](cancellationRequestTopic, cancellationRequested)))

    whenReady(cancelledFuture) {
      _ =>
        val orchestratedEmails = emailOrchestratedConsumer.poll(17000).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails shouldBe empty
        val cancelledComs = cancelledConsumer.poll(20000).records(cancelledTopic).asScala.toList
        cancelledComs.length shouldBe 2
        cancelledComs.map(_.value().get) should contain allOf(Cancelled(triggered1.metadata), Cancelled(triggered2.metadata))
    }
  }

  it should "generate a cancellationFailed event if unable to deschedule a comm" taggedAs DockerComposeTag in {
    val triggered = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(15).toString))
    val commName = triggered.metadata.commManifest.name
    val customerId = triggered.metadata.customerId

    // Create an invalid schedule record
    dynamoClient.putItem(
      new PutItemRequest(
        tableName,
        Map[String, AttributeValue](
          "scheduleId" -> new AttributeValue("scheduleId123"),
          "customerId" -> new AttributeValue(customerId),
          "status" -> new AttributeValue("Pending"),
          "commName" -> new AttributeValue(commName)
        ).asJava
      )
    )
    val genericMetadata = GenericMetadata(triggered.metadata.createdAt, triggered.metadata.eventId, triggered.metadata.traceToken, triggered.metadata.source, false)

    val triggeredFuture = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    Thread.sleep(1000)
    val cancellationRequested = CancellationRequested(genericMetadata, commName, customerId)
    val cancelledFuture = triggeredFuture.flatMap{ t =>
      cancelationRequestedProducer.send(new ProducerRecord[String, CancellationRequested](cancellationRequestTopic, cancellationRequested))
    }

      whenReady(cancelledFuture){
      _ =>
        val failedCancellations = failedCancellationConsumer.poll(20000).records(failedCancellationTopic).asScala.toList
          failedCancellations.length shouldBe 1
          failedCancellations.map(_.value().get) should contain (FailedCancellation(cancellationRequested, "Cancellation of scheduled comm failed: Failed to deserialise pending schedule"))
        val cancelledComs = cancelledConsumer.poll(20000).records(cancelledTopic).asScala.toList
        cancelledComs.length shouldBe 1
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
    if (notStarted) fail("Services did not start within 10 seconds")

    if (!AdminUtils.topicExists(zkUtils, failedTopic)) AdminUtils.createTopic(zkUtils, failedTopic, 1, 1)
    if (!AdminUtils.topicExists(zkUtils, emailOrchestratedTopic)) AdminUtils.createTopic(zkUtils, emailOrchestratedTopic, 1, 1)
    if (!AdminUtils.topicExists(zkUtils, cancellationRequestTopic)) AdminUtils.createTopic(zkUtils, cancellationRequestTopic, 1, 1)
    if (!AdminUtils.topicExists(zkUtils, failedCancellationTopic)) AdminUtils.createTopic(zkUtils, failedCancellationTopic, 1, 1)

    failedCancellationConsumer.assign(Seq(new TopicPartition(failedCancellationTopic, 0)).asJava)
    failedCancellationConsumer.poll(5000).records(failedCancellationTopic).asScala.toList
    commFailedConsumer.assign(Seq(new TopicPartition(failedTopic, 0)).asJava)
    commFailedConsumer.poll(5000).records(failedTopic).asScala.toList
    emailOrchestratedConsumer.assign(Seq(new TopicPartition(emailOrchestratedTopic, 0)).asJava)
    emailOrchestratedConsumer.poll(5000).records(emailOrchestratedTopic).asScala.toList
    cancelationRequestedConsumer.assign(Seq(new TopicPartition(cancellationRequestTopic, 0)).asJava)
    cancelationRequestedConsumer.poll(5000).records(cancellationRequestTopic).asScala.toList
    cancelledConsumer.assign(Seq(new TopicPartition(cancelledTopic, 0)).asJava)
    cancelledConsumer.poll(5000).records(cancelledTopic).asScala.toList

  }

  def createOKCustomerProfileResponse() {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")), StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient.when(
      request()
        .withMethod("GET")
        .withPath(s"/api/customers/GT-CUS-994332344")
        .withQueryStringParameter("apikey", "someApiKey")
    ).respond(
      response(validResponseJson)
        .withStatusCode(200)
    )
  }

  def createInvalidCustomerProfileResponse() {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_missing_required_fields_response.json")), StandardCharsets.UTF_8)

    mockServerClient.reset()
    mockServerClient.when(
      request()
        .withMethod("GET")
        .withPath(s"/api/customers/GT-CUS-994332344")
        .withQueryStringParameter("apikey", "someApiKey")
    ).respond(
      response(validResponseJson)
        .withStatusCode(200)
    )
  }

  def createBadCustomerProfileResponse() {
    mockServerClient.reset()
    mockServerClient.when(
      request()
        .withMethod("GET")
        .withPath(s"/api/customers/GT-CUS-994332344")
        .withQueryStringParameter("apikey", "someApiKey")
    ).respond(
      response("Some error")
        .withStatusCode(500)
    )
  }

  def createFlakyCustomerProfileResponse() {
    val validResponseJson =
      new String(Files.readAllBytes(Paths.get("src/test/resources/profile_valid_response.json")), StandardCharsets.UTF_8)

    mockServerClient.reset()

    // Fail 3 times, then always succeed after that

    mockServerClient.when(
      request()
        .withMethod("GET")
        .withPath(s"/api/customers/GT-CUS-994332344")
        .withQueryStringParameter("apikey", "someApiKey"),
      Times.exactly(3)
    ).respond(
      response("Some error")
        .withStatusCode(500)
    )

    mockServerClient.when(
      request()
        .withMethod("GET")
        .withPath(s"/api/customers/GT-CUS-994332344")
        .withQueryStringParameter("apikey", "someApiKey")
    ).respond(
      response(validResponseJson)
        .withStatusCode(200)
    )
  }

  private def createTable() = {
    val secondaryIndices = Seq(
      SecondaryIndexData("customerId-commName-index", Seq('customerId -> S, 'commName -> S)),
      SecondaryIndexData("status-orchestrationExpiry-index", Seq('status -> S, 'orchestrationExpiry -> N))
    )
    LocalDynamoDB.createTableWithSecondaryIndex(dynamoClient, tableName)(Seq('scheduleId -> S))(secondaryIndices)
    waitUntilTableMade(50)

    def waitUntilTableMade(noAttemptsLeft: Int): String ={
      try{
        val tableStatus = dynamoClient.describeTable(tableName).getTable.getTableStatus
        if (tableStatus != "ACTIVE" && noAttemptsLeft > 0){
          Thread.sleep(100)
          waitUntilTableMade(noAttemptsLeft -1)
        } else tableName
      } catch {
        case _: AmazonDynamoDBException =>
          Thread.sleep(100)
          waitUntilTableMade(noAttemptsLeft -1)
      }
    }
  }


}
