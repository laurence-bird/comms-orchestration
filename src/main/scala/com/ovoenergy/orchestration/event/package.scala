package com.ovoenergy.orchestration
import com.ovoenergy.comms.model.TriggeredV3
import shapeless.lens

package object event {

  implicit val triggeredV3MetadataLens = lens[TriggeredV3].metadata
}
