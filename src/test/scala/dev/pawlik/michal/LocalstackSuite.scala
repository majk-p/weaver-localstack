package dev.pawlik.michal

import cats.Endo
import cats.effect.IO
import cats.effect.Resource
import cats.implicits.*
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.LocalStackV2Container.Service
import com.dimafeng.testcontainers.SingleContainer
import io.laserdisc.pure.kinesis.tagless.KinesisAsyncClientOp
import io.laserdisc.pure.kinesis.tagless.{Interpreter => KinesisInterpreter}
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp
import io.laserdisc.pure.s3.tagless.{Interpreter => S3Interpreter}
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3ClientBuilder
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import weaver.*

import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Random

object LocalstackSuite extends MutableIOSuite {

  type Res = (
      KinesisAsyncClientOp[IO],
      S3AsyncClientOp[IO],
      String,
      String
  )

  val streamNameBase = "my-stream"
  val bucketNameBase = "my-bucket"

  def sharedResource: Resource[IO, Res] =
    for {
      container <- localstack.utils.runLocalstack(
        Seq(Service.KINESIS, Service.S3)
      )
      kinesisClient <- localstack.utils.kinesisClient(container)
      s3Client      <- localstack.utils.s3Client(container)
      streamName <- localstack.utils.kinesisStreamResource(kinesisClient)(
        streamNameBase
      )
      bucket <- localstack.utils.s3BucketResource(s3Client)(bucketNameBase)
    } yield (
      kinesisClient,
      s3Client,
      streamName,
      bucket
    )

  test("read message from kinesis and write it to s3 bucket").usingRes {
    case (kinesisClient, s3Client, streamName, bucketName) =>
      val messageProcessor = MessageProcessor.instance(
        kinesisClient,
        s3Client,
        streamName,
        bucketName
      )
      val messageContent = "test message content"
      for {
        _ <- kinesisClient.putRecord(
          PutRecordRequest
            .builder()
            .streamName(streamName)
            .data(SdkBytes.fromUtf8String(messageContent))
            .partitionKey("some-partition")
            .build()
        )
        _ <- messageProcessor.process
        listObjectsResponse <- s3Client.listObjects(
          ListObjectsRequest.builder().bucket(bucketName).build()
        )
        objects <- listObjectsResponse.contents().asScala.toList.traverse {
          obj =>
            s3Client.getObject(
              GetObjectRequest
                .builder()
                .bucket(bucketName)
                .key(obj.key())
                .build(),
              AsyncResponseTransformer.toBytes[GetObjectResponse]
            )
        }
        s3Objects = objects.map(_.asUtf8String())
      } yield expect.all(s3Objects == List(messageContent))
  }
}
