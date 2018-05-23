package com.ovoenergy.orchestration.scheduling.dynamo

import com.amazonaws.regions.Regions
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.orchestration.aws.AwsProvider
import com.ovoenergy.orchestration.scheduling.{Schedule, ScheduleNew}

object TableMigrator extends App with DynamoFormats {

  val oldUAT = Table[Schedule]("orchestrator-UAT-SchedulingTable-1NMI7H1DMJ49D")
  val newUAT = Table[ScheduleNew]("orchestrator-UAT-SchedulingTableV2-SUSEF7S2MR8W")

  val oldPRD = Table[Schedule]("")
  val newPRD = Table[ScheduleNew]("")

  val dbClient = AwsProvider.dynamoClients(false, Regions.fromName("eu-west-1")).db

  def migrate(oldTable: Table[Schedule], newTable: Table[ScheduleNew]) =
    Scanamo
      .exec(dbClient)(oldTable.scan())
      .map(_.right.get)
      .map(ScheduleNew.buildFromOld)
//      .map(n => Scanamo.exec(dbClient)(newTable.put(n)))
        .foreach(n => println(n))
  migrate(oldUAT, newUAT)
//  migrate(oldPRD, newPRD)

}