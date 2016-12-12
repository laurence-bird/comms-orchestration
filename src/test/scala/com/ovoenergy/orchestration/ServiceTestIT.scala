package com.ovoenergy.orchestration

import org.scalatest.{FlatSpec, Matchers, Tag}

class ServiceTestIT extends FlatSpec
 with Matchers {

  object DockerComposeTag extends Tag("DockerComposeTag")

  behavior of "Service Testing"

  it should "pass a placeholder test" taggedAs DockerComposeTag in {
    true shouldBe true
  }

}
