package dev.pawlik.michal

import cats.effect.IO
import cats.implicits.*
import io.laserdisc.pure.kinesis.tagless.KinesisAsyncClientOp
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.jdk.CollectionConverters.*

object aws {
  object extensions {

    extension (kinesisClient: KinesisAsyncClientOp[IO]) {
      def readMessages(streamName: String): IO[List[String]] =
        for {
          shardsResponse <- kinesisClient.listShards(
            ListShardsRequest.builder().streamName(streamName).build()
          )
          shards = shardsResponse.shards().asScala.toList
          now <- IO.realTimeInstant
          iteratorsResponses <- shards.parTraverse { shard =>
            kinesisClient.getShardIterator(
              GetShardIteratorRequest
                .builder()
                .streamName(streamName)
                .shardId(shard.shardId())
                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                .timestamp(now)
                .build()
            )
          }
          iterators = iteratorsResponses.map(_.shardIterator())
          recordsResponses <- iterators.parTraverse { iterator =>
            kinesisClient.getRecords(
              GetRecordsRequest.builder().shardIterator(iterator).build()
            )
          }
          messages = recordsResponses
            .flatMap(_.records().asScala)
            .map(_.data().asUtf8String())
        } yield messages
    }

    extension (s3Client: S3AsyncClientOp[IO]) {
      def putMessage(bucketName: String)(message: String) =
        for {
          objectKey <- IO.randomUUID.map(_.toString())
          request = PutObjectRequest
            .builder()
            .bucket(bucketName)
            .key(objectKey)
            .build()
          body = AsyncRequestBody.fromString(message)
          _ <- s3Client.putObject(request, body)
        } yield ()
    }
  }
}
