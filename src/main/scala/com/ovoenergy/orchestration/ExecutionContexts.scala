package com.ovoenergy.orchestration

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

trait ExecutionContexts {

  val executionContext         = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val blockingExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

}
