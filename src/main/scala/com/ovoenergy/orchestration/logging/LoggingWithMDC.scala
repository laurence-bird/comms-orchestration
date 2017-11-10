package com.ovoenergy.orchestration.logging

import cats.kernel.Monoid
import cats.implicits._
import com.ovoenergy.comms.model.LoggableEvent
import org.slf4j.{LoggerFactory, MDC}

trait LoggingWithMDC {

  protected def loggerName: String = getClass.getSimpleName.reverse.dropWhile(_ == '$').reverse

  lazy val log = LoggerFactory.getLogger(loggerName)

  protected def logDebug(traceToken: String, message: String): Unit = {
    log(Map("traceToken" -> traceToken), () => log.debug(message))
  }

  protected def logDebug(event: LoggableEvent, message: String): Unit = {
    log(event.mdcMap, () => log.debug(message))
  }

  protected def logInfo(traceToken: String, message: String): Unit = {
    log(Map("traceToken" -> traceToken), () => log.info(message))
  }

  protected def logInfo(event: LoggableEvent, message: String): Unit = {
    log(event.mdcMap, () => log.info(message))
  }

  protected def logInfo(event: LoggableEvent, message: String, mdcParams: Map[String, String]): Unit = {
    log(Monoid.combine(event.mdcMap, mdcParams), () => log.info(message))
  }

  protected def logWarn(traceToken: String, message: String): Unit = {
    log(Map("traceToken" -> traceToken), () => log.warn(message))
  }

  protected def logWarn(traceToken: String, message: String, error: Throwable): Unit = {
    log(Map("traceToken" -> traceToken), () => log.warn(message, error))
  }

  protected def logWarn(event: LoggableEvent, message: String, error: Throwable): Unit = {
    log(event.mdcMap, () => log.warn(message, error))
  }

  protected def logError(traceToken: String, message: String): Unit = {
    log(Map("traceToken" -> traceToken), () => log.error(message))
  }

  protected def logError(traceToken: String, message: String, error: Throwable): Unit = {
    log(Map("traceToken" -> traceToken), () => log.error(message, error))
  }

  protected def logError(event: LoggableEvent, message: String, error: Throwable): Unit = {
    log(event.mdcMap, () => log.error(message, error))
  }

  private def log(mdcMap: Map[String, String], loggingFunction: () => Unit) {
    try {
      mdcMap.foreach { case (k, v) => MDC.put(k, v) }
      loggingFunction()
    } finally {
      mdcMap.foreach { case (k, _) => MDC.remove(k) }
    }

  }

}
