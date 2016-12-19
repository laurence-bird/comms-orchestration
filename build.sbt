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

