package servicetest.helpers

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.ovoenergy.comms.model.{CommManifest, TemplateManifest}
import com.ovoenergy.comms.templates.s3.S3Prefix
import com.ovoenergy.comms.templates.util.Hash

trait FakeS3Configuration {

  private def getPrefix(templateManifest: TemplateManifest) = {
    S3Prefix.fromTemplateManifest(templateManifest)
  }

  private val fragmentObjects = List(
    ("ovo-comms-templates", "fragments/email/html/header.html", "HTML HEADER"),
    ("ovo-comms-templates", "fragments/email/txt/header.txt", "TEXT HEADER"),
    ("ovo-comms-templates", "fragments/sms/txt/header.txt", "SMS HEADER")
  )

  private def emailObjects(templateManifest: TemplateManifest) = {
    val prefix = getPrefix(templateManifest)
    List(
      ("ovo-comms-templates", s"$prefix/email/subject.txt", "SUBJECT {{profile.firstName}}"),
      ("ovo-comms-templates", s"$prefix/email/body.html", "{{> header}} HTML BODY {{amount}}"),
      ("ovo-comms-templates", s"$prefix/email/body.txt", "{{> header}} TEXT BODY {{amount}}")
    )
  }

  private def printObjects(templateManifest: TemplateManifest) = List(
    ("ovo-comms-templates",
     s"${getPrefix(templateManifest)}/print/body.html",
     "Hello, this is a letter. Give me {{amount}} plz")
  )

  private def smsObjects(templateManifest: TemplateManifest) = List(
    ("ovo-comms-templates", s"${getPrefix(templateManifest)}/sms/body.txt", "{{> header}} SMS BODY {{amount}}")
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

  def uploadTemplateToFakeS3(region: String, s3Endpoint: String)(templateManifest: TemplateManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)
    s3.createBucket("ovo-comms-templates")
    removeExistingTemplateObjects(s3, templateManifest)
    emailObjects(templateManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))
    smsObjects(templateManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))
    printObjects(templateManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  def uploadSMSOnlyTemplateToFakeS3(region: String, s3Endpoint: String)(templateManifest: TemplateManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    removeExistingTemplateObjects(s3, templateManifest)
    smsObjects(templateManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  def uploadPrintOnlyTemplateToFakeS3(region: String, s3Endpoint: String)(templateManifest: TemplateManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    removeExistingTemplateObjects(s3, templateManifest)
    printObjects(templateManifest).foreach(s3Object => s3.putObject(s3Object._1, s3Object._2, s3Object._3))

    Thread.sleep(100)
  }

  private def removeExistingTemplateObjects(s3: AmazonS3Client, templateManifest: TemplateManifest) = {
    emailObjects(templateManifest).foreach(s3Object => s3.deleteObject(s3Object._1, s3Object._2))
    smsObjects(templateManifest).foreach(s3Object => s3.deleteObject(s3Object._1, s3Object._2))
    Thread.sleep(1000)
  }

}
