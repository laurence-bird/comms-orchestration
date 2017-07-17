# comms-orchestration

[![CircleCI](https://circleci.com/gh/ovotech/comms-orchestration.svg?style=svg)](https://circleci.com/gh/ovotech/comms-orchestration)

Enriches communication events with customer data and determines the best channel to issue the communication over.

Also handles scheduling of comms for delivery in the future.

## Running it locally

The following environment variables are required to run the service locally:
* KAFKA_HOSTS
  * Hosts in the format host1:9092,host2:9092
* PROFILE_SERVICE_API_KEY
  * API Key used to access the customer profile service
* PROFILE_SERVICE_HOST
  * Host of the customer profile service

You can run the service directly with SBT via `sbt run`

### Docker

The docker image can be pushed to your local repo via `sbt docker:publishLocal`

## Tests

### Unit Test

Tests are executed via `sbt test`

### Service Tests

Service tests can be executed via `sbt servicetest:test`.

They work by spinning up an instance of the service and all of its dependencies (Kafka, etc) in Docker containers.

## Deployment

The service is deployed continuously to both the UAT and PRD environments via the [CircleCI build](https://circleci.com/gh/ovotech/comms-orchestration) 

## Credstash

This service uses credstash for secret management, and this dependency is required if you want to publish the docker container for this project locally or to a remote server, or run the service tests. Information on how to install credstash can be found in the [Credstash readme](https://github.com/fugue/credstash)

