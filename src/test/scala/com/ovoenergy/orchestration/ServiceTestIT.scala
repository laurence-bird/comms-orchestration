package com.ovoenergy.orchestration

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import cakesolutions.kafka.KafkaConsumer.{Conf => KafkaConsumerConf}
import cakesolutions.kafka.KafkaProducer.{Conf => KafkaProducerConf}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model
import com.ovoenergy.comms.model.ErrorCode.{InvalidProfile, OrchestrationError, ProfileRetrievalFailed}
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.serialisation.Serialisation._
import com.ovoenergy.comms.serialisation.Decoders._
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleStatus}
import util.{LocalDynamoDB, TestUtil}
import io.circe.generic.auto._
import org.apache.kafka.clients.producer.ProducerRecord
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

  val mockServerClient = new MockServerClient("localhost", 1080)

  val kafkaHosts = "localhost:29092"
  val zookeeperHosts = "localhost:32181"

  val consumerGroup = Random.nextString(10)
  val triggeredV1Producer = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[Triggered], kafkaHosts))
  val triggeredProducer = KafkaProducer(KafkaProducerConf(new StringSerializer, avroSerializer[TriggeredV2], kafkaHosts))
  val commFailedConsumer = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[Failed], kafkaHosts, consumerGroup))
  val emailOrchestratedConsumer = KafkaConsumer(KafkaConsumerConf(new StringDeserializer, avroDeserializer[OrchestratedEmailV2], kafkaHosts, consumerGroup))

  val dynamoUrl = "http://localhost:8000"
  val dynamoClient = LocalDynamoDB.client(dynamoUrl)
  val tableName = "scheduling"

  val failedTopic = "comms.failed"
  val triggeredV1Topic = "comms.triggered"
  val triggeredTopic = "comms.triggered.v2"
  val emailOrchestratedTopic = "comms.orchestrated.email.v2"

  behavior of "Service Testing"

  it should "orchestrate emails request to send immediately" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      _ => assertSuccessfulOrchestration()
    }
  }

  it should "generate unique internalTraceTokens" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
    whenReady(future) {
      _ =>
        val firstEvent = assertSuccessfulOrchestration().head
        val secondFuture = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, TestUtil.triggered))
        whenReady(secondFuture) {
          _ =>
            val secondEvent = assertSuccessfulOrchestration().head
            secondEvent.internalMetadata.internalTraceToken should not equal firstEvent.internalMetadata.internalTraceToken
        }
    }
  }

  it should "orchestrate emails requested to be sent in the future" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()
    val triggered = TestUtil.triggered.copy(deliverAt = Some(Instant.now().plusSeconds(10).toString))
    val future = triggeredProducer.send(new ProducerRecord[String, TriggeredV2](triggeredTopic, triggered))
    whenReady(future) {
      _ =>
        //Assert nothing orchestrated
        val orchestratedEmails = emailOrchestratedConsumer.poll(9000).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails shouldBe empty

        assertSuccessfulOrchestration()
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

    val deadline = 30.seconds.fromNow
    var orchestratedEmails =  mutable.ListBuffer[OrchestratedEmailV2]()
    while(deadline.hasTimeLeft && orchestratedEmails.size < 10) {
      orchestratedEmails ++= assertSuccessfulOrchestration(pollTime = 1000, assertTraceToken = false)
    }
    orchestratedEmails.size shouldBe 10
    orchestratedEmails.map(_.metadata.traceToken) should contain allOf("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  }

  def assertSuccessfulOrchestration(pollTime: Long = 20000, assertTraceToken: Boolean = true): Seq[OrchestratedEmailV2] = {
    val orchestratedEmails = emailOrchestratedConsumer.poll(pollTime).records(emailOrchestratedTopic).asScala.toList
    orchestratedEmails should not be 'empty
    orchestratedEmails.foreach(record => {
      val orchestratedEmail = record.value().getOrElse(fail("No record for ${record.key()}"))
      orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
      orchestratedEmail.customerProfile shouldBe model.CustomerProfile("Gary", "Philpott")
      orchestratedEmail.templateData shouldBe TestUtil.templateData
      if (assertTraceToken) orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
    })
    orchestratedEmails.map(_.value().get)
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

  it should "also consume the old Triggered events" taggedAs DockerComposeTag in {
    createOKCustomerProfileResponse()

    val future = triggeredV1Producer.send(new ProducerRecord[String, Triggered](triggeredV1Topic, TestUtil.triggeredV1))
    whenReady(future) {
      _ =>
        val orchestratedEmails = emailOrchestratedConsumer.poll(200000).records(emailOrchestratedTopic).asScala.toList
        orchestratedEmails.foreach(record => {
          val orchestratedEmail = record.value().getOrElse(fail("No record for ${record.key()}"))
          orchestratedEmail.recipientEmailAddress shouldBe "qatesting@ovoenergy.com"
          orchestratedEmail.customerProfile shouldBe model.CustomerProfile("Gary", "Philpott")
          orchestratedEmail.templateData shouldBe TestUtil.templateData
          orchestratedEmail.metadata.traceToken shouldBe TestUtil.traceToken
        })
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

    assertSuccessfulOrchestration()
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
        notStarted = !(AdminUtils.topicExists(zkUtils, triggeredTopic) && AdminUtils.topicExists(zkUtils, triggeredV1Topic))
      } catch {
        case NonFatal(ex) => Thread.sleep(100)
      }
    }
    if (notStarted) fail("Services did not start within 10 seconds")

    if (!AdminUtils.topicExists(zkUtils, failedTopic)) AdminUtils.createTopic(zkUtils, failedTopic, 1, 1)
    if (!AdminUtils.topicExists(zkUtils, emailOrchestratedTopic)) AdminUtils.createTopic(zkUtils, emailOrchestratedTopic, 1, 1)
    commFailedConsumer.assign(Seq(new TopicPartition(failedTopic, 0)).asJava)
    commFailedConsumer.poll(5000).records(failedTopic).asScala.toList
    emailOrchestratedConsumer.assign(Seq(new TopicPartition(emailOrchestratedTopic, 0)).asJava)
    emailOrchestratedConsumer.poll(5000).records(emailOrchestratedTopic).asScala.toList
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
        case e: AmazonDynamoDBException => {
          Thread.sleep(100)
          waitUntilTableMade(noAttemptsLeft -1)
        }
      }
    }
  }


}
