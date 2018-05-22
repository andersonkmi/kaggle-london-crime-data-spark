package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sharpsw.spark.TraceUtil.{timed, timing}

object LondonCrimeDataExercise001 {
  val sparkSession: SparkSession = SparkSession.builder.appName("LondonCrimeDataExercise001").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    val (headerColumns, contents) = timed("Step 1 - reading contents", readContents(fileContents))
    headerColumns.foreach(println)
    contents.take(10).foreach(println)
    println(timing)
  }

  def readContents(contents: RDD[String]): (List[String], DataFrame) = {
    val headerColumns = contents.first().split(",").toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }

  def row(line: List[String]): Row = {
    //val list = line.head::line.tail.map(_.toDouble)
    val list = List(line(0), line(1), line(2), line(3), line(4).toInt, line(5).toInt, line(6).toInt)
    Row.fromSeq(list)
  }

  def dfSchema(columnNames: List[String]): StructType = {
    //val firstField = StructField(columnNames.head, StringType, nullable = false)
    //val fields = columnNames.map(field => StructField(field, StringType, nullable = false))
    val lsoaCodeField       = StructField(columnNames.head, StringType, nullable = false)
    val boroughField        = StructField(columnNames(1), StringType, nullable = false)
    val majorCategoryField  = StructField(columnNames(2), StringType, nullable = false)
    val minorCategoryField  = StructField(columnNames(3), StringType, nullable = false)
    val valueField          = StructField(columnNames(4), IntegerType, nullable = false)
    val yearField           = StructField(columnNames(5), IntegerType, nullable = false)
    val monthField          = StructField(columnNames(6), IntegerType, nullable = false)

    val fields = List(lsoaCodeField, boroughField, majorCategoryField, minorCategoryField, valueField, yearField, monthField)
    StructType(fields)
  }

}
