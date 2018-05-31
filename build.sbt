import Dependencies._

organization := "org.sharpsw.spark"

name := "data-handling-spark"

val appVersion = "1.0.0"

val appName = "data-handling-spark"

version := appVersion

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  //"org.apache.hadoop" % "hadoop-aws" % "2.7.5",
  //"com.amazonaws" % "aws-java-sdk" % "1.7.4",
  scalaTest % Test
)