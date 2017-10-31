name                  := "orchestration"
organization          := "com.ovoenergy"
scalaVersion          := "2.11.11"
scalacOptions         := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

val circeVersion = "0.7.0"
val commsKafkaSerialisationVersion = "3.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka"          %% "akka-stream-kafka"         % "0.12",
  "com.typesafe.akka"          %% "akka-slf4j"                % "2.3.14",
  "net.cakesolutions"          %% "scala-kafka-client"        % "0.10.0.0",
  "com.ovoenergy"              %% "comms-kafka-messages"      % "1.37",
  "com.ovoenergy"              %% "comms-kafka-serialisation" % commsKafkaSerialisationVersion,
  "com.ovoenergy"              %% "comms-kafka-helpers"       % commsKafkaSerialisationVersion,
  "com.ovoenergy"              %% "comms-templates"           % "0.12",
  "ch.qos.logback"             % "logback-classic"            % "1.1.7",
  "me.moocar"                  % "logback-gelf"               % "0.2",
  "io.logz.logback"            % "logzio-logback-appender"    % "1.0.11",
  "org.typelevel"              %% "cats-core"                 % "0.9.0",
  "com.squareup.okhttp3"       % "okhttp"                     % "3.4.2",
  "io.circe"                   %% "circe-core"                % circeVersion,
  "io.circe"                   %% "circe-shapes"              % circeVersion,
  "io.circe"                   %% "circe-generic-extras"      % circeVersion,
  "io.circe"                   %% "circe-parser"              % circeVersion,
  "io.circe"                   %% "circe-generic"             % circeVersion,
  "org.quartz-scheduler"       % "quartz"                     % "2.2.3",
  "com.gu"                     %% "scanamo"                   % "0.9.1",

  "org.apache.kafka"           %% "kafka"                     % "0.10.2.1" % Test,
  "org.scalacheck"             %% "scalacheck"                % "1.13.4" % Test,
  "org.scalatest"              %% "scalatest"                 % "3.0.3" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.4" % Test,
  "org.mock-server"            % "mockserver-client-java"     % "3.10.4" % Test,

  "com.whisk" %% "docker-testkit-scalatest" % "0.9.3" % ServiceTest,
  "com.whisk" %% "docker-testkit-impl-docker-java" % "0.9.3" % ServiceTest,
  "com.ovoenergy" %% "comms-kafka-test-helpers" % commsKafkaSerialisationVersion % ServiceTest,
  "commons-io" % "commons-io" % "2.5" % ServiceTest
)

resolvers ++= Seq(
  Resolver.bintrayRepo("ovotech", "maven"),
  Resolver.bintrayRepo("cakesolutions", "maven"),
  "confluent-release" at "http://packages.confluent.io/maven/"
)

enablePlugins(JavaServerAppPackaging, DockerPlugin)

commsPackagingMaxMetaspaceSize := 256
commsPackagingHeapSize := 512

test in Test := (test in Test).dependsOn(startDynamoDBLocal).value
testOptions in Test += dynamoDBLocalTestCleanup.value
val testWithDynamo = taskKey[Unit]("start dynamo, run the tests, shut down dynamo")
testWithDynamo := Def.sequential(
  startDynamoDBLocal,
  test in Test,
  stopDynamoDBLocal
).value

// Make ScalaTest write test reports that CircleCI understands
val testReportsDir = sys.env.getOrElse("CI_REPORTS", "target/reports")
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", testReportsDir)

lazy val ServiceTest = config("servicetest") extend(Test)
configs(ServiceTest)
inConfig(ServiceTest)(Defaults.testSettings)
inConfig(ServiceTest)(Seq(
  (parallelExecution in test) := false,
  (parallelExecution in testOnly) := false
))
(test in ServiceTest) := (test in ServiceTest).dependsOn(publishLocal in Docker).value

val scalafmtAll = taskKey[Unit]("Run scalafmt in non-interactive mode with no arguments")
scalafmtAll := {
  import org.scalafmt.bootstrap.ScalafmtBootstrap
  streams.value.log.info("Running scalafmt ...")
  ScalafmtBootstrap.main(Seq("--non-interactive"))
  streams.value.log.info("Done")
}
(compile in Compile) := (compile in Compile).dependsOn(scalafmtAll).value
