import Dependencies._

organization := "org.sharpsw.spark"

name := "kaggle-london-crime-data-spark"

val appVersion = "1.2.2"

val appName = "kaggle-london-crime-data-spark"

version := appVersion

scalaVersion := "2.13.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.json4s" %% "json4s-jackson" % "4.0.6",
  "org.codecraftlabs.spark" %% "spark-utils" % "1.2.9",
  "org.codecraftlabs.aws" %% "aws-utils" % "1.0.0",
  scalaTest % Test
)