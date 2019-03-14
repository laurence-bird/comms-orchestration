package com.ovoenergy.orchestration
package scheduling
package dynamo

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

import cats.effect._
import cats.implicits._

import org.scalacheck.Shapeless._
import org.scalatest.{FlatSpec, Matchers}

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.aws.AwsProvider.DbClients
import com.ovoenergy.orchestration.scheduling.Persistence.{AlreadyBeingOrchestrated, Successful}
import com.ovoenergy.orchestration.scheduling._
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoPersistence.Context
import com.ovoenergy.orchestration.util.{ArbGenerator, ArbInstances, LocalDynamoDB}
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData

import com.ovoenergy.comms.dockertestkit.{DynamoDbKit, ManagedContainers}
import com.ovoenergy.comms.dockertestkit.dynamoDb.DynamoDbClient
import com.ovoenergy.comms.dockertestkit.Model.TableName

class DynamoPersistenceSpec 
  extends IntegrationSpec 
  with ArbInstances 
  with DynamoDbClient
  with DynamoDbKit {

  implicit val ec = ExecutionContext.global
  implicit val cs = IO.contextShift(ec)

  override def managedContainers = ManagedContainers(dynamoDbContainer)

  behavior of "Persistence"

  it should "store scheduled comm" in {

    val scheduledComm = generate[Schedule].copy(status = ScheduleStatus.Pending)

    val pending = testResources.use { case (async, sync, _) =>
      for {
        _ <- async.storeSchedule[IO](scheduledComm)
        pending <- IO(sync.listPendingSchedules())
      } yield pending
    }.unsafeRunSync()

    pending.size shouldBe 1
    pending.head shouldBe scheduledComm           
  }

  it should "return pending schedules" in {

    testResources.use { case (async, sync, now) =>

      val completedSchedule = generate[Schedule].copy(status = ScheduleStatus.Complete)
      val pendingSchedule   = generate[Schedule].copy(status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.plusSeconds(60 * 10))  

      for {
        _ <- async.storeSchedule[IO](completedSchedule)
        _ <- async.storeSchedule[IO](pendingSchedule)
        _ <- async.storeSchedule[IO](expiredOrchestratingSchedule)
        _ <- async.storeSchedule[IO](inProgressOrchestratingSchedule)
        pending <- IO(sync.listPendingSchedules())
      } yield pending should contain only(pendingSchedule)
    }.unsafeRunSync()
  }

  it should "return expired orchestrating schedules" in {

    testResources.use { case (async, sync, now) => 
      val completedSchedule = generate[Schedule].copy(status = ScheduleStatus.Complete)
      val pendingSchedule   = generate[Schedule].copy(status = ScheduleStatus.Pending)
      val expiredOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.minusSeconds(60 * 30))
      val inProgressOrchestratingSchedule = generate[Schedule].copy(status = ScheduleStatus.Orchestrating, orchestrationExpiry = now.plusSeconds(60 * 10))
        
      for {
        _ <- async.storeSchedule[IO](completedSchedule)
        _ <- async.storeSchedule[IO](pendingSchedule)
        _ <- async.storeSchedule[IO](expiredOrchestratingSchedule)
        _ <- async.storeSchedule[IO](inProgressOrchestratingSchedule)
        pending <- IO(sync.listExpiredSchedules())
      } yield pending should contain only(expiredOrchestratingSchedule)
    
    }.unsafeRunSync()
  }

  it should "correctly mark a schedule as orchestrating" in {

    testResources.use { case (async, sync, now) =>
      val schedule = generate[Schedule].copy(status = ScheduleStatus.Pending, history = Seq())
      
      for {
        _ <- async.storeSchedule[IO](schedule)
        result <- IO(sync.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString))
        orchestratingSchedule <- async.retrieveSchedule[IO](schedule.scheduleId).map(_.toRight(new RuntimeException("It is not defined"))).flatMap(IO.fromEither _)
      } yield {
        result shouldBe Successful(schedule.copy(status = ScheduleStatus.Orchestrating,history = Seq(Change(now, "Start orchestrating")),orchestrationExpiry = now.plusSeconds(60 * 5)))
        orchestratingSchedule.status shouldBe ScheduleStatus.Orchestrating
        orchestratingSchedule.orchestrationExpiry shouldBe now.plusSeconds(60 * 5)
      }
    }.unsafeRunSync()  
  }

  it should "not mark a schedule as orchestrating that is already orchestrating" in {
    
    testResources.use { case (async, sync, now) =>
      val orchestrationExpiry = now.plusSeconds(60 * 2)
      val schedule = generate[Schedule].copy(
        status = ScheduleStatus.Orchestrating,
        orchestrationExpiry = orchestrationExpiry
      )

      for {
        _ <- async.storeSchedule[IO](schedule)
        result <- IO(sync.attemptSetScheduleAsOrchestrating(schedule.scheduleId.toString))
        orchestratingSchedule <- async.retrieveSchedule[IO](schedule.scheduleId).map(_.toRight(new RuntimeException("It is not defined"))).flatMap(IO.fromEither _)
      } yield {
        result shouldBe AlreadyBeingOrchestrated
        orchestratingSchedule.status shouldBe ScheduleStatus.Orchestrating
        orchestratingSchedule.orchestrationExpiry shouldBe orchestrationExpiry
      }
    }.unsafeRunSync()
  }

  it should "mark schedules as failed" in {

    testResources.use { case (async, sync, now) =>
      val schedule = generate[Schedule]

      for {
        _ <- async.storeSchedule[IO](schedule)
        _ <- IO(sync.setScheduleAsFailed(schedule.scheduleId.toString, "Invalid profile"))
        result <- async.retrieveSchedule[IO](schedule.scheduleId).map(_.toRight(new RuntimeException("It is not defined"))).flatMap(IO.fromEither _)
      } yield {
        result.status shouldBe ScheduleStatus.Failed
        result.history should contain(Change(now, "Failed - Invalid profile"))
      }
    }.unsafeRunSync()
  }

  it should "mark schedules as complete" in {

    testResources.use { case (async, sync, now) =>
      val schedule = generate[Schedule]

      for {
        _ <- async.storeSchedule[IO](schedule)
        _ <- IO(sync.setScheduleAsComplete(schedule.scheduleId.toString))
        result <- async.retrieveSchedule[IO](schedule.scheduleId).map(_.toRight(new RuntimeException("It is not defined"))).flatMap(IO.fromEither _)
      } yield {
        result.status shouldBe ScheduleStatus.Complete
        result.history should contain(Change(now, "Orchestration complete"))
      }
    }.unsafeRunSync()
  }

  val testResources = for {
    blockingEc <- blockingExecutionContextResource
    client <- dynamoDbClientResource[IO]()
    tableName <- scheduledTableResource(client)
    now <- Resource.liftF(IO(Instant.now()))
  } yield {
    val ctx = Context(client, tableName)
    val clock = Clock.fixed(now, ZoneId.of("UTC"))
    (new AsyncPersistence(ctx, blockingEc, clock), new DynamoPersistence(5.minutes, ctx, clock), now)
  }

  def blockingExecutionContextResource: Resource[IO, ExecutionContext] = {
    Resource.make(IO(Executors.newCachedThreadPool()))(threads => IO(threads.shutdown())).map(ExecutionContext.fromExecutor)
  }

  def scheduledTableResource(client: AmazonDynamoDBAsync): Resource[IO, String] = {
    
    val tableName = "scheduler-test"

    val globalSecondaryIndices = Seq(
      new GlobalSecondaryIndex()
            .withIndexName("status-orchestrationExpiry-index")
            .withKeySchema(
              new KeySchemaElement("status", KeyType.HASH),
              new KeySchemaElement("orchestrationExpiry", KeyType.RANGE)
            )
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
    )


    def createTable: IO[CreateTableResult] = {
      IO.async[CreateTableResult] { cb =>
        client.createTableAsync(new CreateTableRequest(
          List(
            new AttributeDefinition("scheduleId", ScalarAttributeType.S),
            new AttributeDefinition("status", ScalarAttributeType.S),
            new AttributeDefinition("orchestrationExpiry", ScalarAttributeType.N),
          ).asJava,
          tableName, 
          List(new KeySchemaElement("scheduleId", KeyType.HASH)).asJava,
          new ProvisionedThroughput(1L, 1L)
        ).withGlobalSecondaryIndexes(globalSecondaryIndices.asJava),
          new AsyncHandler[CreateTableRequest, CreateTableResult] {
            override def onError(exception: Exception): Unit = cb(exception.asLeft)
            override def onSuccess(request: CreateTableRequest, result: CreateTableResult): Unit =
              cb(result.asRight)
          }
        )

        ()
      }.onError {
        case NonFatal(e) => 
          IO(println(s"Error creting DynamoDb table: ${e}"))
      }
    }

    def removeTable: IO[DeleteTableResult] = {
      IO.async[DeleteTableResult] { cb =>
        client.deleteTableAsync(
          tableName,
          new AsyncHandler[DeleteTableRequest, DeleteTableResult] {
            override def onError(exception: Exception): Unit =
              cb(exception.asLeft)
            override def onSuccess(request: DeleteTableRequest, result: DeleteTableResult): Unit =
              cb(result.asRight)
          }
        )

        ()
      }.onError {
        case NonFatal(e) => 
          IO(println(s"Error deleting DynamoDb table: ${e}"))
      }
    }


    Resource.make(createTable.as(tableName))(_ => removeTable.void)
  }
}
