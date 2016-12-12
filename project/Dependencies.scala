import sbt._

object Dependencies {
  val kafkaMessagesVersion = "0.0.13"

  def all() = Seq(
    "com.typesafe.akka"   %%  "akka-http-core"            % "10.0.0",
    "com.typesafe.akka"    %% "akka-stream-kafka"         % "0.12",
    "net.cakesolutions"   %%  "scala-kafka-client"        % "0.10.0.0",
    "com.ovoenergy"       %%  "comms-kafka-messages"      % kafkaMessagesVersion,
    "com.ovoenergy"       %%  "comms-kafka-serialisation" % kafkaMessagesVersion,
    "com.typesafe.akka"   %%  "akka-slf4j"                % "2.3.14",
    "ch.qos.logback"       %  "logback-classic"           % "1.1.7",
    "me.moocar"            %  "logback-gelf"              % "0.2",
    "io.logz.logback"      %  "logzio-logback-appender"   % "1.0.11",
    "org.scalatest"       %%  "scalatest"                 % "2.2.6"     % Test
  )

}
