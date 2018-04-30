import Dependencies._

organization := "org.sharpsw.Spark"

name := "data-handling-spark"

val appVersion = "1.0.0"

val appName = "data-handling-spark"

version := appVersion

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.1",
  //"org.apache.hadoop" % "hadoop-aws" % "2.7.5",
  //"com.amazonaws" % "aws-java-sdk" % "1.7.4",
  scalaTest % Test
)

//assemblyJarName in assembly := appName + "-" + appVersion + ".jar"
