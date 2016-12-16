# comms-orchestration

[![CircleCI](https://circleci.com/gh/ovotech/comms-orchestration.svg?style=svg)](https://circleci.com/gh/ovotech/comms-orchestration)

Encriches communication events with customer data and determines the best channel to issue the communication over.

Determined channel restricted to emails for now. At some point in the future it will support other channels, e.g. SMS, push. Will also allow communications to scheduled in the future.

## Running it locally

The following environment variables are required to run the service locally:
* KAFKA_HOSTS
  * Hosts in the format host1:9092,host2:9092
* CANARY_EMAIL_ADDRESS
  * Email address to issue canary (test) communications to
* PROFILE_SERVICE_API_KEY
  * API Key used to access the customer profile service
* PROFILE_SERVICE_HOST
  * Host of the customer profile service

You can run the service directly with SBT via `sbt run`

### Docker

The docker image can be pushed to your local repo via `sbt docker:publishLocal`

The docker-compose.yml file included in the project is used for service testing and if used will spin up a kafka/zookeeper instance and a mock http server that the service will then interact with, this is not really suitable for manually testing with (stick with `sbt run`).

## Tests

### Unit Test

Tests are executed via `sbt test`

### Service Tests

[Service tests] (https://github.com/ovotech/comms-orchestration/blob/master/src/test/scala/com/ovoenergy/orchestration/ServiceTestIT.scala) execute the service as a 'black box' using docker-compose, as described above.

Service tests can be execute via `sbt dockerComposeTest`

If you wish to execute the service tests in your IDE then run `sbt dockerComposeUp` and run the tests in your IDE.

## Deployment

The service is deployed continuously to both the UAT and PRD environments via the [CircleCI build](https://circleci.com/gh/ovotech/comms-orchestration) 

## Credstash

This service uses credstash for secret management, and this dependency is required if you want to publish the docker container for this project locally or to a remote server, or run the docker-compose tests. Information on how to install credstash can be found in the [Credstash readme](https://github.com/fugue/credstash)

