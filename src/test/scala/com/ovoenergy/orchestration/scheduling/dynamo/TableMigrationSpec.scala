package com.ovoenergy.orchestration.scheduling.dynamo

import com.ovoenergy.orchestration.scheduling.ScheduleNew
import com.ovoenergy.orchestration.util.ArbGenerator
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Shapeless._

class TableMigrationSpec extends FlatSpec with Matchers with ArbGenerator {

  it should "slice by batch size" in {
    val lst = (1 to 1024).toList.map(x => generate[ScheduleNew])
    TableMigration.slice(lst).map(_.size) shouldBe Seq(24, 500, 500)
  }

}
