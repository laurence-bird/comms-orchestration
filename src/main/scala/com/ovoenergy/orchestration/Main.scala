package com.ovoenergy.orchestration

import java.io.File
import java.nio.file.Files

import com.ovoenergy.orchestration.logging.LoggingWithMDC

import collection.JavaConverters._

object Main extends App
  with LoggingWithMDC {

  override def loggerName = "Main"

  Files.readAllLines(new File("banner.txt").toPath).asScala.foreach(println(_))

  //Doing nothing, just making the service "run"
  while (true) {
    Thread.sleep(1000)
  }

  log.info("Orchestration started")
}
