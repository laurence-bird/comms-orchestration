package com.ovoenergy.orchestration.serviceTest.util

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.ovoenergy.comms.model.CommManifest

trait FakeS3Configuration {

  def uploadTemplateToS3(region: String, s3Endpoint: String)(commManifest: CommManifest): Unit = {
    // disable chunked encoding to work around https://github.com/jubos/fake-s3/issues/164
    val s3clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()

    val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials("key", "secret"))
      .withRegion(Regions.fromName(region))

    s3.setS3ClientOptions(s3clientOptions)
    s3.setEndpoint(s3Endpoint)

    s3.createBucket("ovo-comms-templates")

    // template
    s3.putObject("ovo-comms-templates",
                 s"service/${commManifest.name}/${commManifest.version}/email/subject.txt",
                 "SUBJECT {{profile.firstName}}")
    s3.putObject("ovo-comms-templates",
                 s"service/${commManifest.name}/${commManifest.version}/email/body.html",
                 "{{> header}} HTML BODY {{amount}}")
    s3.putObject("ovo-comms-templates",
                 s"service/${commManifest.name}/${commManifest.version}/email/body.txt",
                 "{{> header}} TEXT BODY {{amount}}")
    s3.putObject("ovo-comms-templates",
                 s"service/${commManifest.name}/${commManifest.version}/sms/body.txt",
                 "{{> header}} SMS BODY {{amount}}")

    // fragments
    s3.putObject("ovo-comms-templates", "service/fragments/email/html/header.html", "HTML HEADER")
    s3.putObject("ovo-comms-templates", "service/fragments/email/txt/header.txt", "TEXT HEADER")
    s3.putObject("ovo-comms-templates", "service/fragments/sms/txt/header.txt", "SMS HEADER")

    Thread.sleep(100)
  }

}
