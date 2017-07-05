import sbt._

object Dependencies {

  val circeVersion = "0.7.0"

  def all() = Seq(
    "com.typesafe.akka"          %% "akka-stream-kafka"         % "0.12",
    "com.typesafe.akka"          %% "akka-slf4j"                % "2.3.14",
    "net.cakesolutions"          %% "scala-kafka-client"        % "0.10.0.0",
    "com.ovoenergy"              %% "comms-kafka-messages"      % "1.22",
    "com.ovoenergy"              %% "comms-kafka-serialisation" % "2.8",
    "com.ovoenergy"              %% "comms-templates"           % "0.6",
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
    "org.apache.kafka"           %% "kafka"                     % "0.10.0.1" % Test,
    "org.scalacheck"             %% "scalacheck"                % "1.13.4" % Test,
    "org.scalatest"              %% "scalatest"                 % "2.2.6" % Test,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.4" % Test,
    "org.mock-server"            % "mockserver-client-java"     % "3.10.4" % Test
  )

}
