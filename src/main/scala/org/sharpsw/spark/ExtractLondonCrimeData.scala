package org.sharpsw.spark

import java.nio.file.FileSystems.getDefault
import java.nio.file.Files.exists
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sharpsw.spark.utils.DataFrameUtil.{extractDistinctValues, saveDataFrameToCsv, saveDataFrameToParquet}
import org.sharpsw.spark.utils.TraceUtil.{timed, timing}

object ExtractLondonCrimeData {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting Extract London Crime data information")

    val sparkSession: SparkSession = if(args.length == 3) SparkSession.builder.appName("ExtractLondonCrimeData").master(args(2)).getOrCreate() else SparkSession.builder.appName("ExtractLondonCrimeData").getOrCreate()
    val defaultFS = getDefault

    if(args.nonEmpty && exists(defaultFS.getPath(args(0)))) {
      val destinationFolder = (if(args.length == 2 || args.length == 3) args(1) else ".") + defaultFS.getSeparator

      Logger.getLogger("org.apache").setLevel(Level.OFF)

      logger.info(s"Reading ${args(0)} contents")
      val fileContents = sparkSession.sparkContext.textFile(args(0))
      val (headerColumns, contents) = timed("Reading file contents", readContents(fileContents, sparkSession))
      contents.cache()

      logger.info("Printing data set schema information:")
      headerColumns.foreach(logger.info(_))

      logger.info("Extracting distinct boroughs")
      val boroughs = timed("Extracting distinct boroughs", extractDistinctBoroughs(contents))
      timed("Exporting boroughs to csv", saveDataFrameToCsv(boroughs, s"${destinationFolder}borough.csv"))
      timed("Exporting the boroughs data frame to parquet", saveDataFrameToParquet(boroughs, s"${destinationFolder}borough.parquet"))

      logger.info("Extracting LSOA codes by borough")
      val lsoa = timed("Extracting LSOA codes by borough", extractBoroughLsoa(contents))
      timed("Exporting lsoa codes to csv", saveDataFrameToCsv(lsoa, s"${destinationFolder}lsoa.csv"))
      timed("Exporting lsoa codes to parquet", saveDataFrameToParquet(lsoa, s"${destinationFolder}lsoa.parquet"))

      logger.info("Extracting distinct major crime categories")
      val majorCrimeCategories = timed("Extracting major categories", extractDistinctMajorCrimeCategories(contents))
      timed("Exporting major categories to csv", saveDataFrameToCsv(majorCrimeCategories, s"${destinationFolder}major_category.csv"))
      timed("Exporting major categories to parquet", saveDataFrameToParquet(majorCrimeCategories, s"${destinationFolder}major_category.parquet"))

      logger.info("Extracting distinct minor crime categories")
      val minorCrimeCategories = timed("Extracting minor categories", extractDistinctMinorCrimeCategories(contents))
      timed("Exporting minor category to csv", saveDataFrameToCsv(minorCrimeCategories, s"${destinationFolder}minor_category.csv"))
      timed("Exporting minor category to parquet", saveDataFrameToParquet(minorCrimeCategories, s"${destinationFolder}minor_category.parquet"))

      logger.info("Extracting distinct combined crime categories")
      val categories = timed("Extracting categories to csv", extractCombinedCategories(contents))
      timed("Exporting categories to csv", saveDataFrameToCsv(categories, s"${destinationFolder}categories.csv"))
      timed("Exporting categories to parquet", saveDataFrameToParquet(categories, s"${destinationFolder}categories.parquet"))

      logger.info("Calculating total crimes by borough")
      val crimesByBorough = timed("Calculate total crimes by borough", calculateTotalCrimeCountByBorough(contents))
      timed("Exporting resulting aggregation to CSV", saveDataFrameToCsv(crimesByBorough, s"${destinationFolder}total_crimes_by_borough.csv"))
      timed("Exporting resulting aggregation to parquet", saveDataFrameToParquet(crimesByBorough, s"${destinationFolder}total_crimes_by_borough.parquet"))

      logger.info("Calculating total crimes by major category")
      val crimesByMajorCategory = timed("Calculate total crimes by major category", calculateCrimesByMajorCategory(contents))
      timed("Exporting resulting aggregation - by major category", saveDataFrameToCsv(crimesByMajorCategory, s"${destinationFolder}total_crimes_by_major_category.csv"))
      timed("Exporting resulting aggregation - by major category", saveDataFrameToParquet(crimesByMajorCategory, s"${destinationFolder}total_crimes_by_major_category.parquet"))

      logger.info("Calculating total crimes by minor category")
      val crimesByMinorCategory = timed("Calculate total crimes by minor category", calculateCrimeCountByMinorCategory(contents))
      timed("Exporting resulting aggregation - by minor category", saveDataFrameToCsv(crimesByMinorCategory, s"${destinationFolder}total_crimes_by_minor_category.csv"))
      timed("Exporting resulting aggregation - by minor category", saveDataFrameToParquet(crimesByMinorCategory, s"${destinationFolder}total_crimes_by_minor_category.parquet"))

      logger.info("Calculating total crimes by borough and year")
      val crimesByBoroughAndYear = timed("Calculate total crimes by borough and year", calculateCrimeCountByBoroughAndYear(contents))
      timed("Exporting resulting aggregation - by borough and year", saveDataFrameToCsv(crimesByBoroughAndYear, s"${destinationFolder}total_crimes_by_borough_year.csv"))
      timed("Exporting resulting aggregation - by borough and year", saveDataFrameToParquet(crimesByBoroughAndYear, s"${destinationFolder}total_crimes_by_borough_year.parquet"))

      logger.info("Calculating total crimes by major category and year")
      val crimesByMajorCategoryAndYear = timed("Calculate total crimes by major category and year", calculateCrimesByMajorCategoryAndYear(contents))
      timed("Exporting resulting aggregation - by major category and year", saveDataFrameToCsv(crimesByMajorCategoryAndYear, s"${destinationFolder}total_crimes_by_major_category_year.csv"))
      timed("Exporting resulting aggregation - by major category and year", saveDataFrameToParquet(crimesByMajorCategoryAndYear, s"${destinationFolder}total_crimes_by_major_category_year.parquet"))

      logger.info("Calculating total crimes by minor category and year")
      val crimesByMinorCategoryAndYear = timed("Calculate total crimes by minor category and year", calculateCrimesByMinorCategoryAndYear(contents))
      timed("Exporting resulting aggregation - by minor category and year", saveDataFrameToCsv(crimesByMinorCategoryAndYear, s"${destinationFolder}total_crimes_by_minor_category_year.csv"))
      timed("Exporting resulting aggregation - by minor category and year", saveDataFrameToParquet(crimesByMinorCategoryAndYear, s"${destinationFolder}total_crimes_by_minor_category_year.parquet"))

      logger.info("Calculating total crimes by year")
      val crimesByYear = timed("Calculate total crimes by year", calculateCrimesByYear(contents))
      timed("Exporting crimes by year results", saveDataFrameToCsv(crimesByYear, s"${destinationFolder}total_crimes_by_year.csv"))
      timed("Exporting crimes by year results", saveDataFrameToParquet(crimesByYear, s"${destinationFolder}total_crimes_by_year.parquet"))

      logger.info("Calculating total crimes by year and month")
      val crimesByYearMonth = timed("Calculate total crimes by year", calculateCrimesByYearAndMonth(contents))
      timed("Exporting crimes by year and month results", saveDataFrameToCsv(crimesByYearMonth, s"${destinationFolder}total_crimes_by_year_month.csv"))
      timed("Exporting crimes by year and month results", saveDataFrameToParquet(crimesByYearMonth, s"${destinationFolder}total_crimes_by_year_month.parquet"))

      logger.info("Percentages of crimes by years")
      val crimePercentageByYear = calculateCrimesPercentageByCategoryAndYear(contents, sparkSession)
      crimePercentageByYear.foreach(item => {
        logger.info(s"Exporting crime percentage for year '${item._1}'")
        timed(s"Exporting crime percentage in ${item._1} to CSV", saveDataFrameToCsv(item._2, s"${destinationFolder}crime_percentage_${item._1}.csv"))
        timed(s"Exporting crime percentage in ${item._1} to parquet", saveDataFrameToParquet(item._2, s"${destinationFolder}crime_percentage_${item._1}.parquet"))
      })
      println(timing)

      logger.info("Exiting Extract London Crime data information")
    } else {
      logger.error("Missing arguments.")
    }
  }

  def readContents(contents: RDD[String], sparkSession: SparkSession): (List[String], DataFrame) = {
    val headerColumns = contents.first().split(",").toList
    val schema = dfSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
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
