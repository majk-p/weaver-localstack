import sbt._

object Dependencies {
  lazy val catsEffect = Seq(
    "org.typelevel" %% "cats-effect" % "3.5.2"
  )
  lazy val fs2Aws = Seq(
    "io.laserdisc" %% "fs2-aws-s3",
    "io.laserdisc" %% "fs2-aws-kinesis"
  ).map(_ % "6.1.1")
  lazy val testcontainersLocalstack =
    Seq(
      "com.dimafeng" %% "testcontainers-scala-localstack-v2" % "0.41.0"
    ).map(_ % Test)
  lazy val weaver =
    Seq("com.disneystreaming" %% "weaver-cats" % "0.8.3").map(_ % Test)
}
