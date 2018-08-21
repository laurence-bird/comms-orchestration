package com.ovoenergy.comms.orchestration.aws

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBAsync,
  AmazonDynamoDBAsyncClientBuilder,
  AmazonDynamoDBClientBuilder
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ovoenergy.comms.templates.TemplatesContext
import org.slf4j.LoggerFactory

object AwsProvider {

  private val log = LoggerFactory.getLogger("AwsClientProvider")

  case class DbClients(async: AmazonDynamoDBAsync, db: AmazonDynamoDB)
  def dynamoClients(isRunningInLocalDocker: Boolean, region: Regions): DbClients = {
    if (isRunningInLocalDocker) {
      log.warn("Running in local docker")
      System.setProperty("com.amazonaws.sdk.disableCertChecking", "true")
      val awsCreds = getCreds(isRunningInLocalDocker, region)
      DbClients(
        async = AmazonDynamoDBAsyncClientBuilder
          .standard()
          .withEndpointConfiguration(new EndpointConfiguration(sys.env("LOCAL_DYNAMO"), region.getName))
          .build(),
        db = AmazonDynamoDBClientBuilder
          .standard()
          .withEndpointConfiguration(new EndpointConfiguration(sys.env("LOCAL_DYNAMO"), region.getName))
          .build()
      )
    } else {
      val awsCreds = getCreds(isRunningInLocalDocker, region)
      DbClients(
        async = AmazonDynamoDBAsyncClientBuilder
          .standard()
          .withCredentials(awsCreds)
          .withRegion(region)
          .build(),
        db = AmazonDynamoDBClientBuilder
          .standard()
          .withCredentials(awsCreds)
          .withRegion(region)
          .build()
      )
    }
  }

  def templatesContext(isRunningInLocalDocker: Boolean, region: Regions) = {
    val s3Credentials = getCreds(isRunningInLocalDocker, region)
    TemplatesContext.cachingContext(s3Credentials)
  }

  private def getCreds(isRunningInLocalDocker: Boolean, region: Regions): AWSCredentialsProvider = {
    if (isRunningInLocalDocker) {
      log.info("Running in docker, assoigning static credentials")
      new AWSStaticCredentialsProvider(new BasicAWSCredentials("key", "secret"))
    } else
      new AWSCredentialsProviderChain(
        new ContainerCredentialsProvider(),
        new ProfileCredentialsProvider()
      )
  }
}
