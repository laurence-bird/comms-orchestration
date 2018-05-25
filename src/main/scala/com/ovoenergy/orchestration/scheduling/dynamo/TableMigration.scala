package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import cats.data.Kleisli
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{BatchWriteItemResult, ScanRequest}
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.{Scanamo, ScanamoAsync, Table}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId, ScheduleNew}
import cats.implicits._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import fs2._
import cats.effect.IO
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.ovoenergy.orchestration.util.Retry

object TableMigration extends App with DynamoFormats {

  val start = Instant.now
  println(s"started at ${start}")

  val oldUAT = Table[Schedule]("orchestrator-UAT-SchedulingTable-1NMI7H1DMJ49D")
  val newUAT = Table[ScheduleNew]("orchestrator-UAT-SchedulingTableV2-1QGEJ2251SKT5")

  val oldPRD = Table[Schedule]("")
  val newPRD = Table[ScheduleNew]("")

  val dbClient = AwsProvider.dynamoClients(false, Regions.fromName("eu-west-1"))

  var counter = 1

  def migrateWithFs2(oldTable: Table[Schedule], newTable: Table[ScheduleNew]): Unit = {

    val retry = Retry.backOff()

    val source = Stream.eval(ScanamoFs2.scan[IO].apply(oldTable).run(dbClient.async)).flatMap(identity)

    source
      .map(ScheduleNew.buildFromOld)
      .segmentN(750)
      .evalMap{xs =>
        val toInsert = xs.force.toVector.toSet
        retry(ScanamoF.exec[IO](dbClient.async)(newTable.putAll(toInsert))) >> IO(println(s"Inserted ${toInsert.size} record")) >> IO(counter += toInsert.size)
      }
      .compile
      .last
      .map(_ => ())
      .unsafeRunSync()
  }

  def migrateAsync(oldTable: Table[Schedule], newTable: Table[ScheduleNew]) =
    Scanamo
      .exec(dbClient.db)(oldTable.scan())
      .map(_.right.get)
      .map(ScheduleNew.buildFromOld)
      .map(n => ScanamoAsync.exec(dbClient.async)(newTable.put(n)))
      .map(r => r.map(n => {
        println(s"$counter: ${n}")
        counter += 1
        1
      }))
      .sequence[Future, Int]
      .map(_.reduce(_ + _))
      .map(result => {
        println(s"Total migrated: $result")
        println(s"done at ${LocalDateTime.now}")
      })

  def migrate(oldTable: Table[Schedule], newTable: Table[ScheduleNew]) = {
    val newSchedule =
      Scanamo
        .exec(dbClient.db)(oldTable.scan())
        .map(_.right.get)
        .map(ScheduleNew.buildFromOld)

    slice(newSchedule)
      .map(_.toSet)
      .flatMap(n => {
        n.map(t => Scanamo.exec(dbClient.db)(newTable.put(t)).map(r => r match {
          case Right(sn) => 1
          case Left(error) => {
            Scanamo.exec(dbClient.db)(newTable.put(t))
            0
          }
        }))
      })
      .map(_.get)
      .reduce(_ + _)
  }

  val batchSize = 500

  def slice(input: List[ScheduleNew]): List[List[ScheduleNew]] = {

    def sliceHelper(original: List[ScheduleNew], result: List[List[ScheduleNew]]): List[List[ScheduleNew]] = {
      if (original.size < batchSize) {
        return original :: result
      } else {
        sliceHelper(original.drop(batchSize), original.take(batchSize) :: result)
      }
    }

    sliceHelper(input, List[List[ScheduleNew]]())
  }

  val totalMigrated = migrateWithFs2(oldUAT, newUAT)

  println(s"Total migrated: $totalMigrated")
  println(s"finished at ${Instant.now}")
  println(s"""done in ${Instant.now.minusSeconds(start.getEpochSecond)}""")
}