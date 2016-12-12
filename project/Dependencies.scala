import sbt._

object Dependencies {

  def all() = Seq(
    "com.typesafe.akka"   %%  "akka-slf4j"                % "2.3.14",
    "ch.qos.logback"       %  "logback-classic"           % "1.1.7",
    "me.moocar"            %  "logback-gelf"              % "0.2",
    "io.logz.logback"      %  "logzio-logback-appender"   % "1.0.11",
    "org.scalatest"       %%  "scalatest"                 % "2.2.6"     % Test
  )

}
