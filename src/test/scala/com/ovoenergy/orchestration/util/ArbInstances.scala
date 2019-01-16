package com.ovoenergy.orchestration.util

import java.time.Instant
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.rng.Seed
import org.scalacheck.Shapeless._

import scala.util.Random

trait ArbInstances extends ArbGenerator {

  implicit def arbUUID: Arbitrary[UUID] = Arbitrary {
    UUID.randomUUID()
  }

  // Ensure we don't get empty strings
  implicit def arbString: Arbitrary[String] = Arbitrary {
    UUID.randomUUID().toString
  }

  implicit def arbInstant: Arbitrary[Instant] = Arbitrary {
    Instant.now().plusSeconds(Random.nextInt(5))
  }
}

trait ArbGenerator {

  def generate[A: Arbitrary] =
    implicitly[Arbitrary[A]].arbitrary
      .apply(Gen.Parameters.default.withSize(Random.nextInt(2)), Seed.random())
      .get

}
