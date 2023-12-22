package dev.pawlik.michal.localstack

import cats.Endo
import cats.effect.IO
import cats.effect.Resource
import cats.implicits.*
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.SingleContainer
import io.laserdisc.pure.kinesis.tagless.KinesisAsyncClientOp
import io.laserdisc.pure.kinesis.tagless.{Interpreter => KinesisInterpreter}
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp
import io.laserdisc.pure.s3.tagless.{Interpreter => S3Interpreter}
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3ClientBuilder
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Random

object utils {

  def runLocalstack(
      services: Seq[LocalStackV2Container.Service]
  ): Resource[IO, LocalStackV2Container] =
    containerResource(createContainer(services))

  def kinesisClient(
      container: LocalStackV2Container
  ): Resource[IO, KinesisAsyncClientOp[IO]] =
    KinesisInterpreter
      .apply[IO]
      .KinesisAsyncClientOpResource(
        KinesisAsyncClient
          .builder()
          .endpointOverride(container.endpointOverride(Service.KINESIS))
          .region(container.region)
          .credentialsProvider(container.staticCredentialsProvider)
      )

  def s3Client(
      container: LocalStackV2Container
  ): Resource[IO, S3AsyncClientOp[IO]] =
    S3Interpreter
      .apply[IO]
      .S3AsyncClientOpResource(
        S3AsyncClient
          .builder()
          .endpointOverride(container.endpointOverride(Service.S3))
          .region(container.region)
          .credentialsProvider(container.staticCredentialsProvider)
      )

  def kinesisStreamResource(
      kinesisClient: KinesisAsyncClientOp[IO]
  )(
      streamName: String,
      additionalParameters: Endo[CreateStreamRequest.Builder] = identity
  ): Resource[IO, String] =
    Resource.make(for {
      sn <- IO(Random.alphanumeric.take(8).mkString).map(randomSuffix =>
        s"$streamName-$randomSuffix"
      )
      _ <- kinesisClient.createStream(
        additionalParameters(
          CreateStreamRequest.builder().streamName(sn).shardCount(1)
        ).build()
      )
      _ <- kinesisClient.waiter.flatMap { waiter =>
        val describeStreamRequest =
          DescribeStreamRequest.builder().streamName(sn).build()
        IO.fromFuture(
          IO(waiter.waitUntilStreamExists(describeStreamRequest).asScala)
        )
      }
    } yield sn)(sn =>
      kinesisClient
        .deleteStream(DeleteStreamRequest.builder().streamName(sn).build())
        .void
    )

  def s3BucketResource(
      s3Client: S3AsyncClientOp[IO]
  )(
      bucketNamePrefix: String
  ): Resource[IO, String] =
    Resource.make {
      IO(Random.alphanumeric.take(8).mkString.toLowerCase)
        .map(randomSuffix => s"$bucketNamePrefix-$randomSuffix")
        .flatTap(bucket =>
          s3Client.createBucket(
            CreateBucketRequest.builder().bucket(bucket).build()
          )
        )
    } { bucketName =>
      for {
        leftovers <- s3Client.listObjects(
          ListObjectsRequest.builder().bucket(bucketName).build()
        )
        _ <- leftovers.contents().asScala.toList.traverse { obj =>
          s3Client.deleteObject(
            DeleteObjectRequest
              .builder()
              .bucket(bucketName)
              .key(obj.key())
              .build()
          )
        }
        _ <- s3Client.deleteBucket(
          DeleteBucketRequest.builder().bucket(bucketName).build()
        )
      } yield ()
    }

  private def createContainer(
      services: Seq[LocalStackV2Container.Service]
  ): IO[LocalStackV2Container] =
    IO {
      val localStackTag = "2.3.2"
      LocalStackV2Container(tag = localStackTag, services = services)
        .configure(
          _.setDockerImageName(s"localstack/localstack:$localStackTag")
        )
    }

  private def containerResource[T <: SingleContainer[?]](
      container: IO[T]
  ): Resource[IO, T] =
    Resource.fromAutoCloseable(container.flatTap(c => IO(c.start())))

}
