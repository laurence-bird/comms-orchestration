import sbt._

object Dependencies {
  val kafkaMessagesVersion = "0.0.26-SNAPSHOT"
  val circeVersion = "0.6.1"

  def all() = Seq(
    "com.typesafe.akka"   %% "akka-http-core"            % "10.0.0",
    "com.typesafe.akka"   %% "akka-stream-kafka"         % "0.12",
    "com.typesafe.akka"   %% "akka-slf4j"                % "2.3.14",
    "net.cakesolutions"   %% "scala-kafka-client"        % "0.10.0.0",
    "com.ovoenergy"       %% "comms-kafka-messages"      % kafkaMessagesVersion,
    "com.ovoenergy"       %% "comms-kafka-serialisation" % "1.0",
    "ch.qos.logback"       % "logback-classic"           % "1.1.7",
    "me.moocar"            % "logback-gelf"              % "0.2",
    "io.logz.logback"      % "logzio-logback-appender"   % "1.0.11",
    "org.typelevel"       %% "cats-core"                 % "0.8.1",
    "com.squareup.okhttp3" % "okhttp"                    % "3.4.2",
    "io.circe"            %% "circe-core"                % circeVersion,
    "io.circe"            %% "circe-generic-extras"      % circeVersion,
    "io.circe"            %% "circe-parser"              % circeVersion,
    "io.circe"            %% "circe-generic"             % circeVersion,
    "org.quartz-scheduler" % "quartz"                    % "2.2.3",
    "com.gu" %% "scanamo"                    % "0.8.3",
    "org.apache.kafka"    %% "kafka"                     % "0.10.0.1"  % Test,
    "org.scalacheck"      %% "scalacheck"                % "1.13.4"    % Test,
    "org.scalatest"       %% "scalatest"                 % "2.2.6"     % Test,
    "org.mock-server"      % "mockserver-client-java"    % "3.10.4"    % Test
  )

}
