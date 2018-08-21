package com.ovoenergy.comms.orchestration.scheduling.dynamo

import java.time.{DateTimeException, Instant}
import java.util.UUID
import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import com.ovoenergy.comms.model._
import com.ovoenergy.comms.templates.TemplateMetadataDynamoFormats
import com.ovoenergy.comms.orchestration.scheduling.ScheduleStatus
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.parse
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.{Decoder, Encoder, Error}
trait DynamoFormats extends TemplateMetadataDynamoFormats {

  implicit val uuidDynamoFormat =
    DynamoFormat.coercedXmap[UUID, String, IllegalArgumentException](UUID.fromString)(_.toString)

  implicit val instantDynamoFormat =
    DynamoFormat.coercedXmap[Instant, Long, DateTimeException](Instant.ofEpochMilli)(_.toEpochMilli)

  implicit val scheduleStatusDynamoFormat = DynamoFormat.coercedXmap[ScheduleStatus, String, MatchError] {
    case "Pending"       => ScheduleStatus.Pending
    case "Orchestrating" => ScheduleStatus.Orchestrating
    case "Complete"      => ScheduleStatus.Complete
    case "Failed"        => ScheduleStatus.Failed
    case "Cancelled"     => ScheduleStatus.Cancelled
  } {
    case ScheduleStatus.Pending       => "Pending"
    case ScheduleStatus.Orchestrating => "Orchestrating"
    case ScheduleStatus.Complete      => "Complete"
    case ScheduleStatus.Failed        => "Failed"
    case ScheduleStatus.Cancelled     => "Cancelled"
  }

  implicit val commTypeDynamoFormat = DynamoFormat.coercedXmap[CommType, String, MatchError] {
    case "Service"    => Service
    case "Regulatory" => Regulatory
    case "Marketing"  => Marketing
  } {
    case Service    => "Service"
    case Regulatory => "Regulatory"
    case Marketing  => "Marketing"
  }

  import io.circe.shapes._

  implicit val templateDataDecoder: Decoder[TemplateData] = deriveDecoder[TemplateData]
  implicit val templateDataEncoder: Encoder[TemplateData] = deriveEncoder[TemplateData]

  implicit val templateDataFormat = DynamoFormat.xmap[TemplateData, String](
    (string) => {
      val decodedTemplateData: Either[Error, TemplateData] = for {
        json         <- parse(string).right
        templateData <- templateDataDecoder.decodeJson(json)
      } yield templateData
      decodedTemplateData.left.map(error => {
        TypeCoercionError(error)
      })
    }
  )(
    (templateData) => templateDataEncoder.apply(templateData).spaces2
  )
}

object DynamoFormats extends DynamoFormats
