package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sharpsw.spark.TraceUtil.{timed, timing}

object ExtractLondonCrimeData {
  val sparkSession: SparkSession = SparkSession.builder.appName("LondonCrimeDataExercise001").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    val (headerColumns, contents) = timed("Step 1 - reading contents", readContents(fileContents))
    contents.persist()
    headerColumns.foreach(println)
    contents.printSchema()
    timed("Saving boroughs to csv", extractDistinctBoroughs(contents))
    timed("Saving major categories to csv", extractDistinctMajorCrimeCategories(contents))
    timed("Saving minor categories to csv", extractDistinctMinorCrimeCategories(contents))
    timed("Saving categories to csv", extractCombinedCategories(contents))
    println(timing)
  }

  def readContents(contents: RDD[String]): (List[String], DataFrame) = {
    val headerColumns = contents.first().split(",").toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    //dataFrame.persist
    (headerColumns, dataFrame)
  }

  def extractDistinctBoroughs(contents: DataFrame): Unit = {
    extractDistinctValues(contents, "borough")
  }

  def extractDistinctMajorCrimeCategories(contents: DataFrame): Unit = {
    extractDistinctValues(contents, "major_category")
  }

  def extractDistinctMinorCrimeCategories(contents: DataFrame): Unit = {
    extractDistinctValues(contents, "minor_category")
  }

  def extractCombinedCategories(contents: DataFrame): Unit = {
    val distinctValues = contents.select("major_category", "minor_category").distinct.sort("major_category", "minor_category")
    distinctValues.coalesce(1).write.mode("overwrite").option("header", "true").csv("categories.csv")
  }

  private def extractDistinctValues(contents: DataFrame, columnName: String): Unit = {
    val distinctValues = contents.select(contents(columnName)).distinct.orderBy(asc(columnName))
    distinctValues.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${columnName}.csv")
  }

  def row(line: List[String]): Row = {
    //val list = line.head::line.tail.map(_.toDouble)
    val list = List(line.head, line(1), line(2), line(3), line(4).toInt, line(5).toInt, line(6).toInt)
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
