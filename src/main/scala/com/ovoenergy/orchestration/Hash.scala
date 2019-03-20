package com.ovoenergy.orchestration

import cats._
import cats.implicits._

import java.security.MessageDigest
import java.util.Base64
import com.ovoenergy.comms.model._

trait Hash[F[_]] {
  def apply[A](a: A)(implicit h: Hashable[F, A]): F[String]
}

object Hash {
  def apply[F[_]: Applicative]: Hash[F] = new Hash[F] {
    def apply[A](a: A)(implicit h: Hashable[F, A]): F[String] =
      implicitly[Hashable[F, A]].hash(a)
  }
}

trait Hashable[F[_], A] {
  def hash(a: A): F[String]
}

object Hashable {

  implicit def hashableTriggeredV4[F[_]: FlatMap](
      implicit ae: ApplicativeError[F, Throwable]
  ): Hashable[F, TriggeredV4] = new Hashable[F, TriggeredV4] {
    def hash(a: TriggeredV4): F[String] = {
      ae.catchNonFatal(MessageDigest.getInstance("MD5")).flatMap { md5 =>
        ae.catchNonFatal(
            md5.digest((a.metadata.deliverTo, a.templateData, a.metadata.templateManifest).toString.getBytes))
          .flatMap { hash =>
            Base64.getEncoder().encodeToString(hash).pure[F]
          }
      }
    }
  }
}
