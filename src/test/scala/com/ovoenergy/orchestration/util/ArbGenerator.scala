package com.ovoenergy.orchestration.util

import org.scalacheck.Arbitrary
import org.scalacheck.Shapeless._

trait ArbGenerator {

  def generate[A: Arbitrary] = implicitly[Arbitrary[A]].arbitrary.sample.get

}
