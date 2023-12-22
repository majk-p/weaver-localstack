package dev.pawlik.michal

import cats.effect.IOApp
import cats.effect.IO

import io.laserdisc.pure.kinesis.tagless.KinesisAsyncClientOp
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp

object Main extends IOApp.Simple {

  def run: IO[Unit] = {
    val kinesisClient: KinesisAsyncClientOp[IO] = ???
    val s3Client: S3AsyncClientOp[IO]           = ???
    val kinesisStream                           = "my-stream"
    val s3Bucket                                = "my-bucket"

    val messageProcessor = MessageProcessor.instance(
      kinesisClient,
      s3Client,
      kinesisStream,
      s3Bucket
    )

    fs2.Stream
      .repeatEval(messageProcessor.process)
      .compile
      .drain
  }

}
