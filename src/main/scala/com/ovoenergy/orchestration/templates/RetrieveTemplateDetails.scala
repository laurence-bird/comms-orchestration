package com.ovoenergy.orchestration.templates

import cats.Id
import cats.data.{NonEmptyList, Validated}
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import com.ovoenergy.comms.model.{CommType, TemplateManifest}
import com.ovoenergy.comms.templates.model.template.metadata.TemplateId
import com.ovoenergy.comms.templates._
import com.ovoenergy.comms.templates.model.template.processed.CommTemplate
import cats.syntax.all._
import cats.instances.all._
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import com.ovoenergy.comms.templates.cache.CachingStrategy
import com.ovoenergy.orchestration.logging.LoggingWithMDC
import com.ovoenergy.orchestration.util.Retry

object RetrieveTemplateDetails extends LoggingWithMDC {

  case class TemplateDetails(template: CommTemplate[Id], commType: CommType)

  implicit val ec = scala.concurrent.ExecutionContext.global

  val retry = Retry()

  def apply[F[_]: Async](templatesContext: TemplatesContext,
                         templateMetadataContext: TemplateMetadataContext,
                         cachingStrategy: CachingStrategy[TemplateId, ErrorsOr[CommType]])
    : TemplateManifest => F[ErrorsOr[TemplateDetails]] = { (templateManifest: TemplateManifest) =>
    info(s"Fetching template details for: $templateManifest")
    val templateId = TemplateId(templateManifest.id)

    def template(): ErrorsOr[CommTemplate[Id]] = {
      TemplatesRepo
        .getTemplate(templatesContext, templateManifest)
        .leftMap { error =>
          cachingStrategy.remove(templateId)
          error
        }
    }
    def templateSummary(): ErrorsOr[CommType] = {
      cachingStrategy
        .get(templateId) {
          TemplateMetadataRepo
            .getTemplateSummary(templateMetadataContext, templateId)
            .getOrElse(Invalid(NonEmptyList.of(s"Template summary does not exist for template ${templateManifest.id}")))
            .map(_.commType)
        }
        .leftMap { error =>
          cachingStrategy.remove(templateId)
          error
        }
    }

    val templateDetails: F[ErrorsOr[TemplateDetails]] = for {
      _        <- Async.shift[F](ec)
      commType <- Async[F].delay(templateSummary())
      templ    <- Async[F].delay(template())
      _        <- Async.shift[F](ec)
    } yield {
      (commType, templ)
        .mapN { (commType, template) =>
          TemplateDetails(template, commType)
        }
        .leftMap { errs =>
          warn(s"Failed to retrieve template details: ${errs.toList.mkString(",")}")
          errs
        }
    }

    retry(templateDetails, _.isInstanceOf[ProvisionedThroughputExceededException])
  }
}
