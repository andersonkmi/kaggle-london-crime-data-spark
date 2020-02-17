package org.sharpsw.spark

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.codecraftlabs.spark.utils.DataFrameUtil.extractDistinctValues

object LondonCrimeDataExplorer {
  private val Separator: String = ","
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  private def row(line: List[String]): Row = {
    val list = List(line.head, line(1), line(2), line(3), line(4).toInt, line(5).toInt, line(6).toInt)
    Row.fromSeq(list)
  }

  private def dfSchema(columnNames: List[String]): StructType = {
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

  def readContents(contents: RDD[String], sparkSession: SparkSession): (List[String], DataFrame) = {
    logger.info("Reading file contents")
    val headerColumns = contents.first().split(Separator).toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(Separator).toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }

  def extractDistinctBoroughs(contents: DataFrame): DataFrame = {
    extractDistinctValues(contents, "borough")
  }

  def extractBoroughLsoa(contents: DataFrame): DataFrame = {
    contents.select("borough", "lsoa_code").distinct().sort(asc("borough"), asc("lsoa_code"))
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

  def calculateCrimesByYear(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("year")).agg(sum(contents("value")).alias("total")).sort(desc("year"))
  }

  def calculateCrimesByYearAndMonth(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("year"), contents("month")).agg(sum(contents("value")).alias("total")).sort(desc("year"), desc("month"))
  }

  def calculateCrimePercentageByCategoryByYear(contents: DataFrame, year: Int, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val byMajorCategory = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val filtered = contents.filter($"year" === year)
    filtered.groupBy(filtered("major_category")).agg(sum(filtered("value")).alias("occurrences")).sort(desc("occurrences")).withColumn("total", sum(col("occurrences")).over(byMajorCategory)).withColumn("percentage", col("occurrences")*100/col("total")).drop(col("occurrences")).drop(col("total"))
  }

  def getYearList(contents: DataFrame, sparkSession: SparkSession): List[Int] = {
    import sparkSession.implicits._
    extractDistinctValues(contents, "year").map(i => i.getInt(0)).collect().toList.reverse
  }

  def calculateCrimesPercentageByCategoryAndYear(contents: DataFrame, sparkSession: SparkSession): List[(Int, DataFrame)] = {
    getYearList(contents, sparkSession).map(item => (item, calculateCrimePercentageByCategoryByYear(contents, item, sparkSession)))
  }

  def calculateTotalCrimesByYearLsoaCode(contents: DataFrame): DataFrame = {
    contents.groupBy(contents("year"), contents("borough"), contents("lsoa_code")).agg(sum(contents("value")).alias("total")).sort(desc("year"), asc("borough"), asc("lsoa_code"))
  }
}
