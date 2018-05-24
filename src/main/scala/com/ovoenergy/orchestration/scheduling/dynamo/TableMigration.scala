package com.ovoenergy.orchestration.scheduling.dynamo

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.{Scanamo, ScanamoAsync, Table}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleId, ScheduleNew}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object TableMigration extends App with DynamoFormats {

  val start = Instant.now
  println(s"started at ${start}")

  val oldUAT = Table[Schedule]("orchestrator-UAT-SchedulingTable-1NMI7H1DMJ49D")
  val newUAT = Table[ScheduleNew]("orchestrator-UAT-SchedulingTableV2-1QGEJ2251SKT5")

  val oldPRD = Table[Schedule]("")
  val newPRD = Table[ScheduleNew]("")

  val dbClient = AwsProvider.dynamoClients(false, Regions.fromName("eu-west-1"))

  var counter = 1

  import cats.implicits._

  def migrateAsync(oldTable: Table[Schedule], newTable: Table[ScheduleNew]) =
    Scanamo
      .exec(dbClient.db)(oldTable.scan())
      .map(_.right.get)
      .map(ScheduleNew.buildFromOld)
      .map(n => ScanamoAsync.exec(dbClient.async)(newTable.put(n)))
      .map(r => r.map(n =>{
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
//        val result: immutable.Seq[BatchWriteItemResult] = Scanamo.exec(dbClient.db)(newTable.putAll(n))
//        result.map(x => x.getUnprocessedItems.values().flatten.map(a => a.))
//        println(s"$counter")
//        counter += n.size
//        n.size
      })
      .map(_.get)
      .reduce(_ + _)
  }

  val batchSize = 500
  def slice(input: List[ScheduleNew]): List[List[ScheduleNew]] = {

    def sliceHelper(original: List[ScheduleNew], result: List[List[ScheduleNew]]): List[List[ScheduleNew]] = {
      if(original.size < batchSize) {
        return original :: result
      } else {
        sliceHelper(original.drop(batchSize), original.take(batchSize) :: result)
      }
    }

    sliceHelper(input, List[List[ScheduleNew]]())
  }

//  val totalMigrated = migrate(oldUAT, newUAT)

  val oldSet =
    Scanamo
      .exec(dbClient.db)(oldUAT.scan())
      .map(_.right.get)


//  val old: List[ScheduleId] = oldSet.map(_.scheduleId)
//
//  val next: List[ScheduleId] =
//    Scanamo
//      .exec(dbClient.db)(newUAT.scan())
//      .map(_.right.get)
//      .map(_.scheduleId)
//
//  println(s"Old size: ${old.size}")
//  println(s"New size: ${next.size}")
//
//  old ++ next
//
//  println(old.filter(o => !next.contains(o)))
//  println(s"Total migrated: ${size}")




  val partList =
    List(
      "96ab99fc-7d46-4d29-861b-4c4920e568f5",
      "17174e6e-153f-449a-942a-f130d724fc8d",
      "4618b153-a605-4554-9be6-cdf493840eac",
      "68e891b1-cd5e-4b87-b2c3-8bfe51a8ef97",
      "59454583-e262-4f99-907a-148ce53a43ac",
      "2a9c0c5f-6fbf-47fa-a5b0-35c229ea169a",
      "6e744813-1b85-4e55-a996-b8e3beaf3e9d",
      "3c79bfa7-25ed-4ccd-bc17-de83f5727b72",
      "ff07cbb5-c204-4ed4-b8de-782244c9ac95",
      "50614c80-34e7-427a-a71f-999affa38cef",
      "37734b4f-cc02-44ed-bd15-24ea0b65aab4",
      "3c3e7968-6106-4244-a508-a20ae72c50aa",
      "6feee44f-ed71-4ca6-b250-813771e8c440",
      "9518e1b2-df83-4ded-a9cb-cea3292b4d1e",
      "cc6c739a-0301-4c0a-9c5c-2b11408fc24f",
      "ce16ef42-7e87-4860-89f8-4c2cccd66f2a",
      "dfd809cb-352a-4c5c-97b0-d530c35a7364",
      "dc2adbda-cef2-44d7-a313-a015e1a7e176",
      "93c2c8ea-d8a5-4f96-a402-db6585774cbf",
      "6364e2c2-12d4-4022-be0c-e306cd247d75",
      "28f0eb20-6852-4600-afa7-d65a8957f0f3",
      "1a3cf59b-8c6b-4ceb-adbe-c9e1f5924dc2",
      "77a0173a-a566-4f53-9428-91a8d675553f",
      "cfeb57ab-d104-4671-a85b-b980d145e43e",
      "861f406e-42cb-4dbb-ad52-278f70e6e735",
      "d11068ca-3891-49ec-b9b6-c510d5c9b0a6",
      "5186ad8b-2526-4618-8e32-dd1d32c64661",
      "d7ac6298-5667-4c62-9eea-ad05b7c01e4d",
      "bc832d97-2023-4ce0-a276-ee0e579f9f3d",
      "f96595bb-e6d9-4fb3-b074-67c7548967a3",
      "58921c9d-fe67-4130-a31a-1cb00430aac7",
      "0d58940a-1e6e-4e6f-ad09-dbf2e29fe742",
      "da449e25-4247-4b00-9d5c-f20d6274b657",
      "3cd70e63-fb91-44b2-8c68-a4c74e503fc5",
      "24253eb6-b6a1-4b28-bdd1-09388e0fb2b3",
      "139905e4-526e-42fe-886b-92b4698b2811",
      "65817688-e97d-4000-90fc-837dd67e7203",
      "25d00189-0011-4c06-848d-4b0a5e1b2f0b",
      "bb2b3442-2e1c-445d-8b9b-05f97a107cc4",
      "172e4d91-989e-4ff6-9f3e-09cb18aa6d21",
      "60f2be37-3883-4f8f-8ae4-1087184c8b06",
      "6d584001-62d8-4593-8771-59aff618817e",
      "02178fa9-337f-4afe-804c-c6dba267bfed",
      "e7120ebd-7931-4024-9190-0c4d09dc2d8a"
    )


    oldSet
      .filter(x => partList.contains(x.scheduleId))
      .foreach(println)


  println(s"done at ${Instant.now}")
  println(s"""done in ${Instant.now.minusSeconds(start.getEpochSecond)}""")
}