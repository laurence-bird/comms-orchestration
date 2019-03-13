package com.ovoenergy.orchestration

import org.scalatest.{Matchers, OptionValues, FlatSpec}
import org.scalatest.concurrent.ScaledTimeSpans
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

abstract class IntegrationSpec extends FlatSpec
  with Matchers
  with OptionValues
  with ScaledTimeSpans
  with ScalaCheckDrivenPropertyChecks {
    
    sys.props.put("logback.configurationFile","logback-it.xml")

    override def spanScaleFactor: Double = {
      sys.env.get("TEST_TIME_SCALE_FACTOR")
        .map(_.toDouble)
        .getOrElse(super.spanScaleFactor)
    }

  }