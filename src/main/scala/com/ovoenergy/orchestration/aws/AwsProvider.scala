package com.ovoenergy.orchestration.aws

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.internal.CredentialsEndpointProvider
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

  case class DbClients(async: AmazonDynamoDBAsync, db: AmazonDynamoDB)
}
