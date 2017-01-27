package com.ovoenergy.orchestration.util

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Shapeless._
import org.scalacheck.rng.Seed

import scala.util.Random

trait ArbGenerator {

  def generate[A: Arbitrary] = implicitly[Arbitrary[A]].arbitrary
    .apply(Gen.Parameters.default.withSize(Random.nextInt(5)), Seed.random())
    .get

}
