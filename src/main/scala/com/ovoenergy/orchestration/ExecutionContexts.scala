package com.ovoenergy.orchestration

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait ExecutionContexts {

  val globalExecutionContext   = scala.concurrent.ExecutionContext.global
  val blockingExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
  val akkaExecutionContext     = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

}
