package com.ovoenergy.comms.orchestration.util

import java.time.Instant
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Shapeless._
import org.scalacheck.rng.Seed

import scala.util.Random

trait ArbGenerator {
  implicit def arbUUID: Arbitrary[UUID] = Arbitrary {
    UUID.randomUUID()
  }
  implicit def arbInstant: Arbitrary[Instant] = Arbitrary {
    Instant.now().plusSeconds(Random.nextInt(5))
  }

  // Ensure we don't get empty strings
  implicit def arbString: Arbitrary[String] = Arbitrary {
    UUID.randomUUID().toString
  }

  def generate[A: Arbitrary] =
    implicitly[Arbitrary[A]].arbitrary
      .apply(Gen.Parameters.default.withSize(Random.nextInt(2)), Seed.random())
      .get

}
