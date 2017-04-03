package com.ovoenergy.orchestration.aws

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.ovoenergy.comms.templates.TemplatesContext
import org.slf4j.LoggerFactory

object AwsProvider {

  private val log = LoggerFactory.getLogger("AwsClientProvider")

  def dynamoClient(isRunningInCompose: Boolean, region: Regions): AmazonDynamoDBClient = {
    if (isRunningInCompose) {
      log.warn("Running in compose")
      System.setProperty("com.amazonaws.sdk.disableCertChecking", "true")
      val awsCreds                           = getCreds(isRunningInCompose, region)
      val dynamoClient: AmazonDynamoDBClient = new AmazonDynamoDBClient(awsCreds).withRegion(region)
      dynamoClient.setEndpoint(sys.env("LOCAL_DYNAMO"))
      dynamoClient
    } else {
      val awsCreds = getCreds(isRunningInCompose, region)
      new AmazonDynamoDBClient(awsCreds).withRegion(region)
    }
  }

  def templatesContext(isRunningInCompose: Boolean, region: Regions) = {
    val awsCreds = getCreds(isRunningInCompose, region)
    TemplatesContext.cachingContext(awsCreds)
  }

  private def getCreds(isRunningInCompose: Boolean, region: Regions): AWSCredentialsProvider = {
    if (isRunningInCompose)
      new AWSStaticCredentialsProvider(new BasicAWSCredentials("key", "secret"))
    else
      new AWSCredentialsProviderChain(
        new ContainerCredentialsProvider(),
        new ProfileCredentialsProvider()
      )
  }
}
