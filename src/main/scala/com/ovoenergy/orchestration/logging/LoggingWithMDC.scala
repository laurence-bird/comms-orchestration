package com.ovoenergy.orchestration.logging

import org.slf4j.{LoggerFactory, MDC}

trait LoggingWithMDC {

  def loggerName: String

  lazy val log = LoggerFactory.getLogger(loggerName)

  def logDebug(traceToken: String, message: String): Unit = {
    log(traceToken, () => log.debug(message))
  }

  def logInfo(traceToken: String, message: String): Unit = {
    log(traceToken, () => log.info(message))
  }

  def logWarn(traceToken: String, message: String): Unit = {
    log(traceToken, () => log.warn(message))
  }

  def logError(traceToken: String, message: String): Unit = {
    log(traceToken, () => log.error(message))
  }

  def logError(traceToken: String, message: String, error: Throwable): Unit = {
    log(traceToken, () => log.error(message, error))
  }

  def logWarn(traceToken: String, message: String, error: Throwable): Unit = {
    log(traceToken, () => log.warn(message, error))
  }

  private def log(traceToken: String, loggingFunction: () => Unit) {
    try {
      MDC.put("traceToken", traceToken)
      loggingFunction()
    } finally {
      MDC.remove("traceToken")
    }

  }

}
