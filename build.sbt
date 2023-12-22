import Dependencies._

ThisBuild / scalaVersion := "3.3.1"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "dev.pawlik.michal"

lazy val root = (project in file("."))
  .settings(
    name := "Weaver Localstack",
    testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
    libraryDependencies ++=
      catsEffect ++ fs2Aws ++ weaver ++ testcontainersLocalstack
  )
