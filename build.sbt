name                  := "orchestration"
organization          := "com.ovoenergy"
scalaVersion          := "2.12.4"
scalacOptions         := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

val circeVersion = "0.9.0"
val commsKafkaSerialisationVersion = "3.16"
val dockerTestkitVersion = "0.9.5"
val monocleVersion = "1.5.0"
val fs2KafkaClientVersion = "0.1.9"
val fs2Version = "0.10.3"
val http4sVersion = "0.18.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka"          %% "akka-slf4j"                % "2.4.18",
  "com.ovoenergy"              %% "comms-kafka-messages"      % "1.71"  ,
  "com.ovoenergy"              %% "comms-kafka-serialisation" % commsKafkaSerialisationVersion,
  "com.ovoenergy"              %% "comms-kafka-helpers"       % commsKafkaSerialisationVersion,
  "com.ovoenergy"              %% "comms-templates"           % "0.25",
  "ch.qos.logback"             % "logback-classic"            % "1.1.7",
  "me.moocar"                  % "logback-gelf"               % "0.2",
  "org.slf4j"                  % "jcl-over-slf4j"             % "1.7.25",
  "io.logz.logback"            % "logzio-logback-appender"    % "1.0.11",
  "org.typelevel"              %% "cats-core"                 % "1.0.1",
  "org.typelevel"              %% "cats-effect"               % "0.10",
  "co.fs2"                     %% "fs2-core"                  % fs2Version,
  "com.ovoenergy"              %% "fs2-kafka-client"          % fs2KafkaClientVersion,
  "io.circe"                   %% "circe-core"                % circeVersion,
  "io.circe"                   %% "circe-shapes"              % circeVersion,
  "io.circe"                   %% "circe-generic-extras"      % circeVersion,
  "io.circe"                   %% "circe-parser"              % circeVersion,
  "io.circe"                   %% "circe-generic"             % circeVersion,
  "org.http4s"                 %% "http4s-dsl"                % http4sVersion,
  "org.http4s"                 %% "http4s-blaze-client"       % http4sVersion,
  "org.http4s"                 %% "http4s-circe"              % http4sVersion,
  "org.quartz-scheduler"       % "quartz"                     % "2.2.3",
  "com.gu"                     %% "scanamo"                   % "1.0.0-M3",
  "com.github.tomakehurst"     % "wiremock"                   % "2.16.0" % Test,
  "org.scalacheck"             %% "scalacheck"                % "1.13.4" % Test,
  "org.scalatest"              %% "scalatest"                 % "3.0.3" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.4" % Test,
  "org.mock-server"            % "mockserver-client-java"     % "3.12" % Test,
  "com.github.julien-truffaut" %%  "monocle-core"             % monocleVersion % Test,
  "com.github.julien-truffaut" %%  "monocle-macro"            % monocleVersion % Test,
  "com.github.julien-truffaut" %%  "monocle-law"              % monocleVersion % Test,
  "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion % ServiceTest,
  "com.whisk" %% "docker-testkit-impl-docker-java" % dockerTestkitVersion % ServiceTest,
  "com.whisk" %% "docker-testkit-core"             % dockerTestkitVersion % ServiceTest,
  "com.ovoenergy" %% "comms-kafka-test-helpers" % commsKafkaSerialisationVersion % ServiceTest,
  "commons-io" % "commons-io" % "2.5" % ServiceTest
).map(_.exclude("commons-logging", "commons-logging"))

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
