package servicetest.helpers

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.{Scanamo, Table}
import com.ovoenergy.comms.model.{CommManifest, Service, TemplateManifest}
import com.ovoenergy.comms.templates.model.Brand
import com.ovoenergy.comms.templates.model.template.metadata.{TemplateId, TemplateSummary}
import com.ovoenergy.comms.templates.util.Hash
import com.ovoenergy.orchestration.scheduling.Schedule
import com.ovoenergy.orchestration.scheduling.dynamo.DynamoFormats
import com.ovoenergy.orchestration.util.LocalDynamoDB
import com.ovoenergy.orchestration.util.LocalDynamoDB.SecondaryIndexData

trait DynamoTesting extends DynamoFormats {

  val dynamoUrl                = "http://localhost:8000"
  val dynamoClient             = LocalDynamoDB.client(dynamoUrl)
  val tableName                = "scheduling"
  val templateSummaryTableName = "templateSummaryTable"
  val templateSummaryTable     = Table[TemplateSummary](templateSummaryTableName)

  def createTable(): Unit = {
    val secondaryIndices = Seq(
      SecondaryIndexData("customerId-templateId-index", Seq('customerId  -> S, 'templateId          -> S)),
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

  def listSchedules = {
    Scanamo.exec(dynamoClient)(Table[Schedule](tableName).scan())
  }

  def populateTemplateSummaryTable(ts: TemplateSummary) = {
    Scanamo.exec(dynamoClient)(templateSummaryTable.put(ts))
  }

  def populateTemplateSummaryTable(templateManifest: TemplateManifest) = {
    val ts = TemplateSummary(
      TemplateId(templateManifest.id),
      "blah blah blah",
      Service,
      Brand.Ovo,
      templateManifest.version
    )
    Scanamo.exec(dynamoClient)(templateSummaryTable.put(ts))
  }

  def populateTemplateSummaryTable(commManifest: CommManifest) = {
    val ts = TemplateSummary(
      TemplateId(Hash(commManifest.name)),
      "blah blah blah",
      Service,
      Brand.Ovo,
      commManifest.version
    )
    Scanamo.exec(dynamoClient)(templateSummaryTable.put(ts))
  }

  def createTemplateSummaryTable() = {
    LocalDynamoDB.createTable(dynamoClient)(templateSummaryTableName)('templateId -> S)
    waitUntilTableMade(50)

    def waitUntilTableMade(noAttemptsLeft: Int): (String) = {
      try {
        val summaryTableStatus = dynamoClient.describeTable(templateSummaryTableName).getTable.getTableStatus
        if (summaryTableStatus != "ACTIVE" && noAttemptsLeft > 0) {
          Thread.sleep(20)
          waitUntilTableMade(noAttemptsLeft - 1)
        } else (templateSummaryTableName)
      } catch {
        case e: AmazonDynamoDBException => {
          Thread.sleep(20)
          waitUntilTableMade(noAttemptsLeft - 1)
        }
      }
    }
  }
}
