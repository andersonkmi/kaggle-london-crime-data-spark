package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sharpsw.spark.utils.DataFrameUtil.{saveDataFrame, extractDistinctValues}
import org.sharpsw.spark.utils.TraceUtil.{timed, timing}

object ExtractLondonCrimeData {
  val sparkSession: SparkSession = SparkSession.builder.appName("ExtractLondonCrimeData").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    val (headerColumns, contents) = timed("Step 1 - reading contents", readContents(fileContents))
    contents.persist()
    headerColumns.foreach(println)
    val boroughs = timed("Extracting distinct boroughs", extractDistinctBoroughs(contents))
    timed("Exporting boroughs to csv", saveDataFrame(boroughs, "borough.csv"))

    val majorCrimeCategories = timed("Extracting major categories", extractDistinctMajorCrimeCategories(contents))
    timed("Exporting major categories to csv", saveDataFrame(majorCrimeCategories, "major_category.csv"))

    val minorCrimeCategories = timed("Extracting minor categories", extractDistinctMinorCrimeCategories(contents))
    timed("Exporting minor category to csv", saveDataFrame(minorCrimeCategories, "minor_category.csv"))

    val categories = timed("Extracting categories to csv", extractCombinedCategories(contents))
    timed("Exporting categories to csv", saveDataFrame(categories, "categories.csv"))

    val crimesByBorough = timed("Calculate total crimes by borough", calculateTotalCrimeCountByBorough(contents))
    timed("Exporting resulting aggregation", saveDataFrame(crimesByBorough, "total_crimes_by_borough.csv"))

    val crimesByMajorCategory = timed("Calculate total crimes by major category", calculateCrimesByMajorCategory(contents))
    timed("Exporting resulting aggregation - by major category", saveDataFrame(crimesByMajorCategory, "total_crimes_by_major_category.csv"))

    val crimesByMinorCategory = timed("Calculate total crimes by minor category", calculateCrimeCountByMinorCategory(contents))
    timed("Exporting resulting aggregation - by minor category", saveDataFrame(crimesByMinorCategory, "total_crimes_by_minor_category.csv"))

    val crimesByBoroughAndYear = timed("Calculate total crimes by borough and year", calculateCrimeCountByBoroughAndYear(contents))
    timed("Exporting resulting aggregation - by borough and year", saveDataFrame(crimesByBoroughAndYear, "total_crimes_by_borough_year.csv"))

    val crimesByMajorCategoryAndYear = timed("Calculate total crimes by major category and year", calculateCrimesByMajorCategoryAndYear(contents))
    timed("Exporting resulting aggregation - by major category and year", saveDataFrame(crimesByMajorCategoryAndYear, "total_crimes_by_major_category_year.csv"))

    val crimesByMinorCategoryAndYear = timed("Calculate total crimes by minor category and year", calculateCrimesByMinorCategoryAndYear(contents))
    timed("Exporting resulting aggregation - by minor category and year", saveDataFrame(crimesByMinorCategoryAndYear, "total_crimes_by_minor_category_year.csv"))

    println(timing)
  }

  def readContents(contents: RDD[String]): (List[String], DataFrame) = {
    val headerColumns = contents.first().split(",").toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }

  def extractDistinctBoroughs(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "borough")
  }

  def extractDistinctMajorCrimeCategories(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "major_category")
  }

  def extractDistinctMinorCrimeCategories(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "minor_category")
  }

  def extractCombinedCategories(contents: DataFrame): DataFrame = {
    contents.select("major_category", "minor_category").distinct.sort("major_category", "minor_category")
  }

  def calculateTotalCrimeCountByBorough(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("borough")).agg(sum(contents("value")).alias("total_crimes")).sort(desc("total_crimes"))
  }

  def calculateCrimesByMajorCategory(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("major_category")).agg(sum(contents("value")).alias("total")).sort(desc("total"))
  }

  def calculateCrimeCountByMinorCategory(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("major_category"), contents("minor_category")).agg(sum(contents("value")).alias("total")).sort(desc("total"))
  }

  def calculateCrimeCountByBoroughAndYear(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("borough"), contents("year")).agg(sum(contents("value")).alias("total")).sort(asc("year"), asc("borough"))
  }

  def calculateCrimesByMajorCategoryAndYear(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("major_category"), contents("year")).agg(sum(contents("value")).alias("total")).sort(desc("year"), desc("total"))
  }

  def calculateCrimesByMinorCategoryAndYear(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("major_category"), contents("minor_category"), contents("year")).agg(sum(contents("value")).alias("total")).sort(desc("year"), desc("total"))
  }

  def row(line: List[String]): Row = {
    val list = List(line.head, line(1), line(2), line(3), line(4).toInt, line(5).toInt, line(6).toInt)
    Row.fromSeq(list)
  }

  def dfSchema(columnNames: List[String]): StructType = {
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
