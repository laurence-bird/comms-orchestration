package com.ovoenergy.orchestration.aws

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, AWSStaticCredentialsProvider, BasicAWSCredentials, ContainerCredentialsProvider}
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.s3.AmazonS3Client
import org.slf4j.LoggerFactory

object AwsDynamoClientProvider {

  private val log = LoggerFactory.getLogger("AwsClientProvider")

  def apply(isRunningInCompose: Boolean, region: Regions): AmazonDynamoDBClient = {
      if (isRunningInCompose) {
        log.warn("Running in compose")
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true")
        val awsCreds  = new AWSStaticCredentialsProvider(new BasicAWSCredentials("key", "secret"))
        val dynamoClient: AmazonDynamoDBClient = new AmazonDynamoDBClient(awsCreds).withRegion(region)
        dynamoClient.setEndpoint(sys.env("LOCAL_DYNAMO"))
        dynamoClient
      } else {
        val awsCreds = new AWSCredentialsProviderChain (
          new ContainerCredentialsProvider(),
          new ProfileCredentialsProvider("comms")
        )
        new AmazonDynamoDBClient(awsCreds).withRegion(region)
      }
    }
}
