
// Make ScalaTest write test reports that CircleCI understands
val testReportsDir = sys.env.getOrElse("CI_REPORTS", "target/reports")
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", testReportsDir, "-l", "DockerComposeTag")

lazy val buildSettings = Seq(
  name                  := "orchestration",
  organization          := "com.ovoenergy",
  organizationName      := "OVO Energy",
  organizationHomepage  := Some(url("http://www.ovoenergy.com")),
  scalaVersion          := "2.11.8",
  scalacOptions         := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
)

lazy val service = (project in file("."))
  .settings(buildSettings)
  .settings(resolvers += Resolver.bintrayRepo("ovotech", "maven"))
  .settings(resolvers += Resolver.bintrayRepo("cakesolutions", "maven"))
  .settings(libraryDependencies ++= Dependencies.all)
  .settings(test in Test := (test in Test).dependsOn(startDynamoDBLocal).value)
  .settings(testOptions in Test += dynamoDBLocalTestCleanup.value)
  .settings(testTagsToExecute := "DockerComposeTag")
  .settings(dockerImageCreationTask := (publishLocal in Docker).value)
  .settings(credstashInputDir := file("conf"))
  .settings(variablesForSubstitution := Map("IP_ADDRESS" -> ipAddress))
  .enablePlugins(JavaServerAppPackaging, DockerPlugin, DockerComposePlugin)

lazy val ipAddress: String = {
  val addr = "./get_ip_address.sh".!!.trim
  println(s"My IP address appears to be $addr")
  addr
}

commsPackagingMaxMetaspaceSize := 128

val testWithDynamo = taskKey[Unit]("start dynamo, run the tests, shut down dynamo")
testWithDynamo := Def.sequential(
  startDynamoDBLocal,
  test in Test,
  stopDynamoDBLocal
).value

val scalafmtAll = taskKey[Unit]("Run scalafmt in non-interactive mode with no arguments")
scalafmtAll := {
  import org.scalafmt.bootstrap.ScalafmtBootstrap
  streams.value.log.info("Running scalafmt ...")
  ScalafmtBootstrap.main(Seq("--non-interactive"))
  streams.value.log.info("Done")
}
(compile in Compile) := (compile in Compile).dependsOn(scalafmtAll).value
