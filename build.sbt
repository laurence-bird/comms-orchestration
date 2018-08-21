lazy val root = (project in file(".")).settings(
  commonSettings,
  consoleSettings,
  compilerOptions,
  typeSystemEnhancements,
  formatting,
  dependencies,
  tests,
  packaging
).enablePlugins(JavaServerAppPackaging, DockerPlugin)

def dep(org: String)(version: String)(modules: String*) =
  Seq(modules: _*) map { name =>
    org %% name % version
  }


lazy val commonSettings = Seq(
  name                  := "orchestration",
  organization          := "com.ovoenergy",
  organizationName      := "OVO Energy",
  organizationHomepage  := Some(url("http://www.ovoenergy.com")),
  scalaVersion          := "2.12.6"
)


lazy val consoleSettings = Seq(
  initialCommands := s"import com.ovoenergy.comms.orchestration._",
  scalacOptions in (Compile, console) -= "-Ywarn-unused-import"
)


lazy val compilerOptions =
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-encoding",
    "utf8",
    "-target:jvm-1.8",
    "-feature",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-Ypartial-unification",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"
  )


lazy val typeSystemEnhancements =
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")


lazy val formatting = {
  val scalafmtAll = taskKey[Unit]("Run scalafmt in non-interactive mode with no arguments")

  Seq(
    scalafmtAll := {
      import org.scalafmt.bootstrap.ScalafmtBootstrap
      streams.value.log.info("Running scalafmt ...")
      ScalafmtBootstrap.main(Seq("--non-interactive"))
      streams.value.log.info("Done")} ,
    (compile in Compile) := (compile in Compile).dependsOn(scalafmtAll).value
  )
}


lazy val commsKafkaSerialisationVersion = "3.16"


lazy val dependencies =
  libraryDependencies ++= Seq(
    // core libs: fs2, cats, cats-effect
    dep("co.fs2")("0.10.3")(
      "fs2-core",
      "fs2-io"
    ),
    // Json
    dep("io.circe")("0.9.0")(
      "circe-core",
      "circe-shapes",
      "circe-generic-extras",
      "circe-parser",
      "circe-generic"
    ),
    // Http
    dep("org.http4s")("0.18.9")(
      "http4s-dsl",
      "http4s-blaze-client",
      "http4s-circe"
    ),
    // Internal kafka libs
    Seq(
      "com.ovoenergy" %% "fs2-kafka-client" % "0.1.9",
      "com.ovoenergy" %% "comms-kafka-messages" % "1.71"  ,
      "com.ovoenergy" %% "comms-templates" % "0.28"
    ),
    dep("com.ovoenergy")(commsKafkaSerialisationVersion)(
      "comms-kafka-serialisation",
      "comms-kafka-helpers"
    ),
    // mixed
    Seq(
      "com.typesafe.akka"          %% "akka-slf4j"                % "2.4.18",
      "ch.qos.logback"             % "logback-classic"            % "1.1.7",
      "me.moocar"                  % "logback-gelf"               % "0.2",
      "org.slf4j"                  % "jcl-over-slf4j"             % "1.7.25",
      "io.logz.logback"            % "logzio-logback-appender"    % "1.0.11",
      "org.quartz-scheduler"       % "quartz"                     % "2.2.3",
      "com.gu"                     %% "scanamo"                   % "1.0.0-M3"
    )
  ).flatten.map(_.exclude("commons-logging", "commons-logging"))


lazy val tests = {
  val deps =
    libraryDependencies ++= Seq(
      dep("com.github.julien-truffaut")("1.5.0")(
        "monocle-core",
        "monocle-macro",
        "monocle-law"
      ),
      Seq(
        "com.github.tomakehurst"     % "wiremock"                   % "2.16.0",
        "org.scalacheck"             %% "scalacheck"                % "1.13.4",
        "org.scalatest"              %% "scalatest"                 % "3.0.3",
        "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.4",
        "org.mock-server"            % "mockserver-client-java"     % "3.12"
      )
    ).flatten.map(_.exclude("commons-logging", "commons-logging")).map(_ % Test)

  val dynamo = {
    val testWithDynamo = taskKey[Unit]("start dynamo, run the tests, shut down dynamo")
    Seq(
      testWithDynamo := Def.sequential(
        startDynamoDBLocal,
        test in Test,
        stopDynamoDBLocal
      ).value,
      test in Test := (test in Test).dependsOn(startDynamoDBLocal).value,
      testOptions in Test += dynamoDBLocalTestCleanup.value
    )
  }

  val testDir = {
    // Make ScalaTest write test reports that CircleCI understands
    val testReportsDir = sys.env.getOrElse("CI_REPORTS", "target/reports")
    testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", testReportsDir)
  }

  Seq(testDir, deps) ++ dynamo
}


lazy val packaging = Seq(
  commsPackagingMaxMetaspaceSize := 256,
  commsPackagingHeapSize := 512
)

// service tests

val dockerTestkitVersion = "0.9.5"

libraryDependencies ++= Seq(
  "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion,
  "com.whisk" %% "docker-testkit-impl-docker-java" % dockerTestkitVersion,
  "com.whisk" %% "docker-testkit-core"             % dockerTestkitVersion,
  "com.ovoenergy" %% "comms-kafka-test-helpers" % commsKafkaSerialisationVersion,
  "commons-io" % "commons-io" % "2.5"
).map(_.exclude("commons-logging", "commons-logging")).map(_ % ServiceTest)



lazy val ServiceTest = config("servicetest") extend(Test)
configs(ServiceTest)
inConfig(ServiceTest)(Defaults.testSettings)
inConfig(ServiceTest)(Seq(
  (parallelExecution in test) := false,
  (parallelExecution in testOnly) := false
))
(test in ServiceTest) := (test in ServiceTest).dependsOn(publishLocal in Docker).value
