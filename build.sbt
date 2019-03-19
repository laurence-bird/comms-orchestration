import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.cloudformation.model._

lazy val IT = config("it") extend Test
lazy val ServiceTest = config("servicetest") extend Test

val circeVersion = "0.11.1"
val commsKafkaSerialisationVersion = "3.21"
val commsKafkaMessagesVersion = "1.79.4"
val dockerTestkitVersion = "0.9.5"
val monocleVersion = "1.5.0"
val fs2KafkaVersion = "0.19.4"
val kafkaSerializationVersion = "0.4.1"

val fs2Version = "1.0.4"
val http4sVersion = "0.20.0-M6"
val slf4jVersion = "1.7.26"
val awsSdkVersion = "1.11.490"
val commsDockerkitVersion = "1.8.11"

val cirisVersion = "0.12.1"
val cirisCredstashVersion = "0.6"
val cirisKafkaVersion = "0.13"
val commsDeduplicationVersion = "0.1.7"

inThisBuild(
    List(
      organization := "com.ovoenergy",
      scalaVersion := "2.12.8",
      scalacOptions := Seq(
        "-unchecked",
        "-deprecation",
        "-encoding",
        "utf8",
        "-target:jvm-1.8",
        "-feature",
        "-language:implicitConversions",
        "-language:higherKinds",
        "-language:existentials",
        "-Ypartial-unification"
      ),
      resolvers ++= Seq(
        Resolver.bintrayRepo("ovotech", "maven"),
        Resolver.bintrayRepo("cakesolutions", "maven"),
      "confluent-release" at "http://packages.confluent.io/maven/"
      ),
      scalafmtOnCompile := true,
      dependencyOverrides ++= Seq(
        "com.amazonaws" % "aws-java-sdk-core" % awsSdkVersion,
        "com.amazonaws" % "aws-java-sdk-s3" % awsSdkVersion,
        "com.amazonaws" % "aws-java-sdk-dynamodb" % awsSdkVersion,
        "com.amazonaws" % "aws-java-sdk-kms" % awsSdkVersion
      ),
      excludeDependencies ++= Seq(
        ExclusionRule("commons-logging", "commons-logging"),
        ExclusionRule("org.slf4j", "slf4j-log4j12"),
        ExclusionRule("log4j", "log4j")
      )
    )
)

lazy val orchestration = (project in file("."))
  .enablePlugins(
    BuildInfoPlugin,
    JavaServerAppPackaging,
    AshScriptPlugin,
    DockerPlugin,
    EcrPlugin,
    CloudFormationPlugin
  )
  .configs(IT, ServiceTest)
  .settings(
    name := "orchestrator",
    version ~= (_.replace('+', '-')),
    dynver ~= (_.replace('+', '-')),
  )
  .settings(
    libraryDependencies ++= Seq(
      "com.ovoenergy"              %% "comms-kafka-messages"      % commsKafkaMessagesVersion ,
      "com.ovoenergy"              %% "comms-templates"           % "0.33",
      "is.cir" %% "ciris-core" % cirisVersion,
      "is.cir" %% "ciris-cats" % cirisVersion,
      "is.cir" %% "ciris-cats-effect" % cirisVersion,
      "is.cir" %% "ciris-generic" % cirisVersion,
      "is.cir" %% "ciris-enumeratum" % cirisVersion,
      "com.ovoenergy" %% "ciris-credstash" % cirisCredstashVersion,
      "com.ovoenergy" %% "ciris-aiven-kafka" % cirisKafkaVersion,
      "com.ovoenergy" %% "kafka-serialization-core" % kafkaSerializationVersion,
      "com.ovoenergy" %% "kafka-serialization-avro4s" % kafkaSerializationVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
      "ch.qos.logback"             % "logback-classic"            % "1.2.3",
      "io.logz.logback"            % "logzio-logback-appender"    % "1.0.11",
      "org.typelevel"              %% "cats-core"                 % "1.6.0",
      "org.typelevel"              %% "cats-effect"               % "1.2.0",
      "co.fs2"                     %% "fs2-core"                  % fs2Version,
      "co.fs2"                     %% "fs2-io"                    % fs2Version,
      "com.ovoenergy"              %% "fs2-kafka"                 % fs2KafkaVersion,
      "io.circe"                   %% "circe-core"                % circeVersion,
      "io.circe"                   %% "circe-shapes"              % circeVersion,
      "io.circe"                   %% "circe-generic-extras"      % circeVersion,
      "io.circe"                   %% "circe-parser"              % circeVersion,
      "io.circe"                   %% "circe-generic"             % circeVersion,
      "org.http4s"                 %% "http4s-dsl"                % http4sVersion,
      "org.http4s"                 %% "http4s-blaze-client"       % http4sVersion,
      "org.http4s"                 %% "http4s-circe"              % http4sVersion,
      "org.quartz-scheduler"       % "quartz"                     % "2.2.3",
      "com.gu"                     %% "scanamo"                   % "1.0.0-M8",
      "com.ovoenergy.comms" %% "deduplication" % commsDeduplicationVersion,
      ("com.ovoenergy"              %% "comms-kafka-messages"      % commsKafkaMessagesVersion classifier "tests") % Test,
      "com.github.tomakehurst"     % "wiremock"                   % "2.16.0" % Test,
      "org.scalacheck"             %% "scalacheck"                % "1.14.0" % Test,
      "org.scalatest"              %% "scalatest"                 % "3.0.6" % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.4" % Test,
      "org.mock-server"            % "mockserver-client-java"     % "3.12" % Test,
      "com.github.julien-truffaut" %%  "monocle-core"             % monocleVersion % Test,
      "com.github.julien-truffaut" %%  "monocle-macro"            % monocleVersion % Test,
      "com.github.julien-truffaut" %%  "monocle-law"              % monocleVersion % Test,
      "com.ovoenergy" %% "comms-docker-testkit-core" % commsDockerkitVersion % s"$IT,$ServiceTest",
      "com.ovoenergy" %% "comms-docker-testkit-clients" % commsDockerkitVersion % s"$IT,$ServiceTest",
      "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion % ServiceTest,
      "com.whisk" %% "docker-testkit-impl-docker-java" % dockerTestkitVersion % ServiceTest,
      "com.whisk" %% "docker-testkit-core"             % dockerTestkitVersion % ServiceTest,
      "com.ovoenergy" %% "comms-kafka-test-helpers" % commsKafkaSerialisationVersion % ServiceTest,
      "commons-io" % "commons-io" % "2.5" % ServiceTest,
      "com.ovoenergy"              %% "comms-kafka-serialisation" % commsKafkaSerialisationVersion % ServiceTest,
      "com.ovoenergy"              %% "comms-kafka-helpers"       % commsKafkaSerialisationVersion % ServiceTest,
    ),
  )
  .settings(
    testOptions += Tests.Argument("-oF"),
    inConfig(ServiceTest)(
      Defaults.testSettings ++ Seq(
        parallelExecution := false,
        test := test.dependsOn(publishLocal in Docker).value,
        testOnly := testOnly.dependsOn(publishLocal in Docker).inputTaskValue
      )
    ),
    inConfig(IT)(Defaults.testSettings),
  )
  .settings (
    publishLocal := (Docker / publishLocal).value,
    publish := (Ecr / push).value,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.ovoenergy.comms.orchestrator",

    dockerBaseImage := "openjdk:8-alpine",
    // TODO as we use ECR plugin this is not necessary anymore, the docker repository can be omitted
    dockerRepository := Some("852955754882.dkr.ecr.eu-west-1.amazonaws.com"),
    dockerUpdateLatest := true,

    Ecr / region := Region.getRegion(Regions.EU_WEST_1),
    Ecr / repositoryName := (Docker / packageName).value,
    Ecr / repositoryTags ++= Seq(version.value),
    Ecr / localDockerImage := (Docker / dockerAlias).value.toString,
    Ecr / login := ((Ecr / login) dependsOn (Ecr / createRepository)).value,
    Ecr / push := ((Ecr / push) dependsOn (Docker / publishLocal, Ecr / login)).value,

    cloudFormationCapabilities := Seq(
      Capability.CAPABILITY_IAM
    ),

    Uat / cloudFormationStackParams := Map(
      "Environment" -> "uat",
      "Version" -> version.value
    ),

    Prd / cloudFormationStackParams := Map(
      "Environment" -> "uat",
      "Version" -> version.value
    )
  )

  