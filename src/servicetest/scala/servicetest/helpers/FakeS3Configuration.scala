package servicetest.helpers

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.ovoenergy.comms.model.{CommManifest, TemplateManifest}
import com.ovoenergy.comms.templates.s3.S3Prefix
import com.ovoenergy.comms.templates.util.Hash

trait FakeS3Configuration {

  private def getPrefix(name: String, version: String) = {
    S3Prefix.fromTemplateManifest(TemplateManifest(Hash(name), version))
  }

  private val fragmentObjects = List(
    ("ovo-comms-templates", "fragments/email/html/header.html", "HTML HEADER"),
    ("ovo-comms-templates", "fragments/email/txt/header.txt", "TEXT HEADER"),
    ("ovo-comms-templates", "fragments/sms/txt/header.txt", "SMS HEADER")
  )

  private def emailObjects(commManifest: CommManifest) = {
    val prefix = getPrefix(commManifest.name, commManifest.version)
    List(
      ("ovo-comms-templates", s"$prefix/email/subject.txt", "SUBJECT {{profile.firstName}}"),
      ("ovo-comms-templates", s"$prefix/email/body.html", "{{> header}} HTML BODY {{amount}}"),
      ("ovo-comms-templates", s"$prefix/email/body.txt", "{{> header}} TEXT BODY {{amount}}")
    )
  }

  private def printObjects(commManifest: CommManifest) = List(
    ("ovo-comms-templates",
     s"${getPrefix(commManifest.name, commManifest.version)}/print/body.html",
     "Hello, this is a letter. Give me {{amount}} plz")
  )

  private def smsObjects(commManifest: CommManifest) = List(
    ("ovo-comms-templates",
     s"${getPrefix(commManifest.name, commManifest.version)}/sms/body.txt",
     "{{> header}} SMS BODY {{amount}}")
  )

  def uploadFragmentsToFakeS3(region: String, s3Endpoint: String): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    fragmentObjects.foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  def uploadTemplateToFakeS3(region: String, s3Endpoint: String)(commManifest: CommManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    removeExistingTemplateObjects(s3, commManifest)
    emailObjects(commManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))
    smsObjects(commManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))
    printObjects(commManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  def uploadSMSOnlyTemplateToFakeS3(region: String, s3Endpoint: String)(commManifest: CommManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    removeExistingTemplateObjects(s3, commManifest)
    smsObjects(commManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  def uploadPrintOnlyTemplateToFakeS3(region: String, s3Endpoint: String)(commManifest: CommManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    removeExistingTemplateObjects(s3, commManifest)
    printObjects(commManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  private def removeExistingTemplateObjects(s3: AmazonS3Client, commManifest: CommManifest) = {
    emailObjects(commManifest).foreach(s3Object => s3.deleteObject(s3Object._1, s3Object._2))
    smsObjects(commManifest).foreach(s3Object => s3.deleteObject(s3Object._1, s3Object._2))
    Thread.sleep(1000)
  }

}
