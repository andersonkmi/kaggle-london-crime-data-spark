import Dependencies._

organization := "org.sharpsw.spark"

name := "kaggle-london-crime-data-spark"

val appVersion = "1.2.1"

val appName = "kaggle-london-crime-data-spark"

version := appVersion

scalaVersion := "2.12.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.json4s" %% "json4s-jackson" % "3.6.2",
  "org.codecraftlabs.spark" %% "spark-utils" % "1.0.5",
  scalaTest % Test
)