package servicetest

import org.scalatest.{FlatSpec, Matchers}
import servicetest.helpers.MockProfileResponses

trait BaseSpec extends FlatSpec with Matchers with DockerIntegrationTest with MockProfileResponses
