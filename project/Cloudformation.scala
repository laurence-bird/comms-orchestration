import sbt._
import sbt.Keys._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.collection.immutable.Iterable

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions._
import com.amazonaws.services.cloudformation._
import com.amazonaws.services.cloudformation.model._

object CloudFormationPlugin extends AutoPlugin {

  override def trigger: PluginTrigger = noTrigger

  object autoImport {
    lazy val Prd = config("prd")
    lazy val Uat = config("uat")
  
    lazy val cloudFormationStackName = settingKey[String]("The cloudformation stack name")
    lazy val cloudFormationStackTags = settingKey[Map[String, String]]("The cloudformation stack tags")
    lazy val cloudFormationStackParams = settingKey[Map[String, String]]("The cloudformation stack parameters")
    lazy val cloudFormationStackTemplate = taskKey[File]("The cloudformation stack template file")
    lazy val cloudFormationTemplatesSourceFolder = settingKey[File]("folder where CloudFormation templates are")
    lazy val cloudFormationCapabilities = settingKey[Seq[Capability]]("The cloudformation stack capabilities")
    lazy val cloudFormationDeploy = taskKey[String]("Deploy the stack on cloudfomation")  
  }

  import autoImport._

  override def projectSettings = Seq(
    cloudFormationTemplatesSourceFolder := {sourceDirectory.value / "main" / "aws" },
    cloudFormationCapabilities := Seq(),
  ) ++ projectSettingsInConf(Prd) ++ projectSettingsInConf(Uat)

  def projectSettingsInConf(conf: Configuration) = Seq (
    (cloudFormationStackName in conf) := {name.value ++ "-" ++ conf.toString},
    (cloudFormationStackTags in conf) := {
      Map(
        "team" -> "comms",
        "service" -> name.value,
        "environment" -> conf.toString
      )
    },
    (cloudFormationStackParams in conf) := Map(),
    (cloudFormationStackTemplate in conf) := {cloudFormationTemplatesSourceFolder.value / (name.value ++ ".yml") },
    (cloudFormationDeploy in conf) := {
      val stackName = (cloudFormationStackName  in conf).value
      val template = (cloudFormationStackTemplate in conf).value
      val capabilities = (cloudFormationCapabilities in conf).value
      val tags = (cloudFormationStackTags in conf).value
      val parameters = (cloudFormationStackParams in conf).value
      val log = (streams in cloudFormationDeploy).value.log

      deploy(stackName, template, capabilities, tags, parameters, log)
    }
  )

  private def deploy(
    stackName: String, 
    template: File,
    capabilities: Seq[Capability],
    tags: Map[String, String],
    parameters: Map[String, String],
    log: Logger
  ) = withcloudFormationClient { client =>

    def waitForChangeSet(changeSetId: String) = {
      def getChangeSet: DescribeChangeSetResult = client.describeChangeSet(new DescribeChangeSetRequest().withChangeSetName(changeSetId))
      
      Iterator.iterate(getChangeSet) { _ =>
        getChangeSet
      }.dropWhile{ changeSet => 
        val executionStatus = changeSet.getExecutionStatus()
        val status = changeSet.getStatus()
        val statusReason = changeSet.getStatusReason()
        if(status == "FAILED") {
          log.info(s"ChangeSet has failed: ${statusReason}")
          throw new Exception(s"ChangeSet has failed: $statusReason")
        }
        else if(executionStatus != "AVAILABLE") {
          log.info(s"ChangeSet not yet available: ${status}:${executionStatus}")
          Thread.sleep(1000L)
          true
        } else {
          false
        }
      }.next
    }

    def waitForStack(stackIdOrName: String) {
      def getStack: Option[Stack] = client.describeStacks(new DescribeStacksRequest().withStackName(stackName))
        .getStacks()
        .asScala
        .headOption

      Iterator.iterate(getStack)(_ => getStack)
        .collect { case Some(x) => x}
        .dropWhile { stack => 
          val status = stack.getStackStatus
          val statusReason = stack.getStackStatusReason

          if(status == "CREATE_FAILED") {
            throw new Exception(s"ChangeSet execution has failed: $statusReason")
          } else if (status == "UPDATE_ROLLBACK_COMPLETE" || status == "ROLLBACK_COMPLETE") {
            throw new Exception(s"ChangeSet execution has been rolled back: $statusReason")
          } else if (status == "ROLLBACK_FAILED" || status == "UPDATE_ROLLBACK_FAILED") {
            throw new Exception(s"ChangeSet execution has failed to rollback: $statusReason")
          } else if (status == "CREATE_COMPLETE" || status == "UPDATE_COMPLETE") {
            log.info(s"ChangeSet execution completed")
            false
          } else if (status == "CREATE_IN_PROGRESS" || status == "UPDATE_IN_PROGRESS") {
            log.info(s"ChangeSet execution in progress...")
            Thread.sleep(5000L)
            true
          } else {
            log.info(s"ChangeSet execution in progress, stack status: ${status}")
            Thread.sleep(5000L)
            true
          }
        }.next
    }

    log.info(s"Deploying ${stackName} with parameters: ${parameters} and tags: ${tags}")

    val uniqueId = UUID.randomUUID.toString

    val existing = client
      .describeStacks(new DescribeStacksRequest().withStackName(stackName))
      .getStacks()
      .asScala
      .headOption

    var createChangeSetRequest = existing.fold {

      log.info(s"The stack ${stackName} does not exist, creating it...")

      new CreateChangeSetRequest()
        .withChangeSetType(ChangeSetType.CREATE)
        .withStackName(stackName)
        .withChangeSetName(s"creation-$uniqueId")
        .withCapabilities(capabilities:_*)
        .withParameters(parameters.map{ case (k,v) => new Parameter().withParameterKey(k).withParameterValue(v)}.toList.asJava)
        .withTags(tags.map{ case (k,v) => new Tag().withKey(k).withValue(v)}.toList.asJava)
        .withTemplateBody(IO.read(template))
    } { stack =>

      log.info(s"The stack ${stackName} already exists with id: ${stack.getStackId}, updating it...")

      new CreateChangeSetRequest()
        .withChangeSetType(ChangeSetType.UPDATE)
        .withStackName(stack.getStackId)
        .withChangeSetName(s"update-$uniqueId")
        .withCapabilities(capabilities:_*)
        .withParameters(parameters.map{ case (k,v) => new Parameter().withParameterKey(k).withParameterValue(v)}.toList.asJava)
        .withTags(tags.map{ case (k,v) => new Tag().withKey(k).withValue(v)}.toList.asJava)
        .withTemplateBody(IO.read(template))
    }

    val changeSet = waitForChangeSet(client.createChangeSet(createChangeSetRequest).getId)

    log.info(s"Changeset created with id: ${changeSet.getChangeSetId} and stackId: ${changeSet.getStackId()}, executing it...")

    client.executeChangeSet(new ExecuteChangeSetRequest().withChangeSetName(changeSet.getChangeSetId))
    waitForStack(changeSet.getStackId())

    changeSet.getChangeSetId
  }

  private def withcloudFormationClient[A](f: AmazonCloudFormation => A) = {
    var client = AmazonCloudFormationClientBuilder
      .standard()
      .build()
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }
  
}