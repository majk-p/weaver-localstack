package dev.pawlik.michal

import cats.effect.IO
import cats.implicits.*
import dev.pawlik.michal.aws.extensions.*
import io.laserdisc.pure.kinesis.tagless.KinesisAsyncClientOp
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp

trait MessageProcessor[F[_]] {
  def process: F[Unit]
}

object MessageProcessor {
  def instance(
      kinesisClient: KinesisAsyncClientOp[IO],
      s3Client: S3AsyncClientOp[IO],
      kinesisStreamName: String,
      s3BucketName: String
  ): MessageProcessor[IO] =
    new MessageProcessor[IO] {

      override def process: IO[Unit] =
        for {
          messages <- kinesisClient.readMessages(kinesisStreamName)
          _        <- messages.parTraverse(s3Client.putMessage(s3BucketName))
        } yield ()
    }
}
