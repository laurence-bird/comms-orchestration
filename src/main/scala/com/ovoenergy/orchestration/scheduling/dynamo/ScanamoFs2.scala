package com.ovoenergy.orchestration.scheduling.dynamo

import java.util

import cats.data.Kleisli
import cats.effect.Async
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanResult}
import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.request.{ScanamoQueryOptions, ScanamoScanRequest}
import fs2._
import com.gu.scanamo.error.DynamoReadError

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ScanamoFs2 {

  type Key = Map[String, AttributeValue]

  def scan[F[_]]: PartiallyAppliedScan[F] = new PartiallyAppliedScan[F]

  class PartiallyAppliedScan[F[_]] {

    def apply[A](table: Table[A], from: Option[Key] = None)(
      implicit F: Async[F],
      aFormat: DynamoFormat[A],
      ec: ExecutionContext): Kleisli[F, AmazonDynamoDBAsync, Stream[F, A]] =
      Kleisli { dynamoDBAsync =>

        case class ScanState(key: Option[Key], exhausted: Boolean)

        type Image = util.Map[String, AttributeValue]
        type ImageSegment = Segment[Image, Unit]

        val initialState = ScanState(from, exhausted = false)

        def handleResult(result: ScanResult): Option[(ImageSegment, ScanState)] = {
          val lastEvaluatedKey: Option[Key] =
            Option(result.getLastEvaluatedKey)
              .map(_.asScala.toMap)
              .filter(_.nonEmpty)

          val nextState = lastEvaluatedKey match {
            case x: Some[Key] => ScanState(x, exhausted = false)
            case None         => ScanState(None, exhausted = true)
          }

          val segment = Segment(result.getItems.asScala: _*)

          Option(segment -> nextState)
        }

        val imagesStream: Stream[F, Image] = Stream
          .unfoldSegmentEval[F, ScanState, Image](initialState) {
          case ScanState(_, true) =>
            F.pure(None)
          case ScanState(fromKey, _) =>
            makeScan[F](table.name, fromKey)
              .map(handleResult)
              .run(dynamoDBAsync)
        }

        val aStream: Stream[F, A] = imagesStream.flatMap { image =>
          val aOrError: Either[DynamoReadError, A] =
            aFormat.read(new AttributeValue().withM(image))

          aOrError.fold(
            e => Stream.raiseError[A](new RuntimeException(e.toString)),
            a => Stream.emit(a)
          )
        }

        F.pure(aStream)
      }
  }

  private def makeScan[F[_]: Async](
                                     tableName: String,
                                     lastEvaluatedKey: Option[Key] = None)(implicit ec: ExecutionContext)
  : Kleisli[F, AmazonDynamoDBAsync, ScanResult] = Kleisli { dynamoClient =>

    val queryOptions =
      ScanamoQueryOptions.default.copy(exclusiveStartKey = lastEvaluatedKey)
    val request = ScanamoScanRequest(tableName, index = None, queryOptions)
    val ops: ScanamoOps[ScanResult] = ScanamoOps.scan(request)

    ScanamoF.exec[F](dynamoClient)(ops)
  }
}

object ScanamoF {

  class PartiallyAppliedExec[F[_]](dynamoClient: AmazonDynamoDBAsync) {

    def apply[A](ops: ScanamoOps[A])(implicit F: Async[F], ec: ExecutionContext): F[A] = {
      Async[F].async[A] { cb =>
        val future: Future[A] = ScanamoAsync.exec[A](dynamoClient)(ops)

        future.onComplete {
          case Success(result)    => cb(Right(result))
          case Failure(exception) => cb(Left(exception))
        }
      }
    }
  }

  def exec[F[_]](client: AmazonDynamoDBAsync) = new PartiallyAppliedExec[F](client)
}