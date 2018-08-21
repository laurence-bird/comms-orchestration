package com.ovoenergy.comms.orchestration

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

trait ExecutionContexts {

  val executionContext     = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val akkaExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

}
