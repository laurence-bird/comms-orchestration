package com.ovoenergy.orchestration.serviceTest.util

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.ovoenergy.orchestration.util.LocalDynamoDB
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData
import org.scalatest.{BeforeAndAfterAll, Suite}

trait DynamoTesting {

  val dynamoUrl    = "http://localhost:8000"
  val dynamoClient = LocalDynamoDB.client(dynamoUrl)
  val tableName    = "scheduling"

  def createTable() = {
    val secondaryIndices = Seq(
      SecondaryIndexData("customerId-commName-index", Seq('customerId    -> S, 'commName            -> S)),
      SecondaryIndexData("status-orchestrationExpiry-index", Seq('status -> S, 'orchestrationExpiry -> N))
    )

    if (!LocalDynamoDB.doesTableExist(dynamoClient, tableName)) {
      LocalDynamoDB.createTableWithSecondaryIndex(dynamoClient, tableName)(Seq('scheduleId -> S))(secondaryIndices)
      waitUntilTableMade(50)
    }

    def waitUntilTableMade(noAttemptsLeft: Int): String = {
      try {
        val tableStatus = dynamoClient.describeTable(tableName).getTable.getTableStatus
        if (tableStatus != "ACTIVE" && noAttemptsLeft > 0) {
          Thread.sleep(100)
          waitUntilTableMade(noAttemptsLeft - 1)
        } else tableName
      } catch {
        case _: AmazonDynamoDBException =>
          Thread.sleep(100)
          waitUntilTableMade(noAttemptsLeft - 1)
      }
    }
  }

  def removeTable() = {
    dynamoClient.deleteTable(tableName)
  }

}
