package org.sharpsw.spark

import java.nio.file.FileSystems.getDefault
import java.nio.file.Files.exists
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.sharpsw.spark.utils.DataFrameUtil.{saveDataFrameToCsv, saveDataFrameToParquet}
import org.sharpsw.spark.utils.TraceUtil.{timed, timing}
import LondonCrimeDataExplorer._

object ExtractLondonCrimeData {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting Extract London Crime data information")

    val sparkSession: SparkSession = if(args.length == 3) SparkSession.builder.appName("ExtractLondonCrimeData").master(args(2)).getOrCreate() else SparkSession.builder.appName("ExtractLondonCrimeData").getOrCreate()
    val defaultFS = getDefault

    if(args.nonEmpty && exists(defaultFS.getPath(args(0)))) {
      val destinationFolder = if(args.length == 2 || args.length == 3) args(1) else "."

      Logger.getLogger("org.apache").setLevel(Level.ERROR)

      logger.info(s"Reading ${args(0)} contents")
      val fileContents = sparkSession.sparkContext.textFile(args(0))
      val (headerColumns, contents) = timed("Reading file contents", readContents(fileContents, sparkSession))
      contents.cache()

      logger.info("Printing data set schema information:")
      headerColumns.foreach(logger.info(_))

      logger.info("Extracting distinct boroughs")
      val boroughs = timed("Extracting distinct boroughs", extractDistinctBoroughs(contents))
      timed("Exporting boroughs to csv", saveDataFrameToCsv(boroughs, buildFilePath(destinationFolder, "borough.csv")))
      timed("Exporting the boroughs data frame to parquet", saveDataFrameToParquet(boroughs, buildFilePath(destinationFolder, "bourough.parquet")))

      logger.info("Extracting LSOA codes by borough")
      val lsoa = timed("Extracting LSOA codes by borough", extractBoroughLsoa(contents))
      timed("Exporting lsoa codes to csv", saveDataFrameToCsv(lsoa, buildFilePath(destinationFolder, "lsoa.csv")))
      timed("Exporting lsoa codes to parquet", saveDataFrameToParquet(lsoa, buildFilePath(destinationFolder, "lsoa.parquet")))

      logger.info("Extracting distinct major crime categories")
      val majorCrimeCategories = timed("Extracting major categories", extractDistinctMajorCrimeCategories(contents))
      timed("Exporting major categories to csv", saveDataFrameToCsv(majorCrimeCategories, buildFilePath(destinationFolder, "major_category.csv")))
      timed("Exporting major categories to parquet", saveDataFrameToParquet(majorCrimeCategories, buildFilePath(destinationFolder, "major_category.parquet")))

      logger.info("Extracting distinct minor crime categories")
      val minorCrimeCategories = timed("Extracting minor categories", extractDistinctMinorCrimeCategories(contents))
      timed("Exporting minor category to csv", saveDataFrameToCsv(minorCrimeCategories, buildFilePath(destinationFolder, "minor_category.csv")))
      timed("Exporting minor category to parquet", saveDataFrameToParquet(minorCrimeCategories, buildFilePath(destinationFolder, "minor_category.parquet")))

      logger.info("Extracting distinct combined crime categories")
      val categories = timed("Extracting categories to csv", extractCombinedCategories(contents))
      timed("Exporting categories to csv", saveDataFrameToCsv(categories, buildFilePath(destinationFolder, "categories.csv")))
      timed("Exporting categories to parquet", saveDataFrameToParquet(categories, buildFilePath(destinationFolder, "categories.parquet")))

      logger.info("Calculating total crimes by borough")
      val crimesByBorough = timed("Calculate total crimes by borough", calculateTotalCrimeCountByBorough(contents))
      timed("Exporting resulting aggregation to CSV", saveDataFrameToCsv(crimesByBorough, buildFilePath(destinationFolder, "total_crimes_by_borough.csv")))
      timed("Exporting resulting aggregation to parquet", saveDataFrameToParquet(crimesByBorough, buildFilePath(destinationFolder, "total_crimes_by_borough.parquet")))

      logger.info("Calculating total crimes by major category")
      val crimesByMajorCategory = timed("Calculate total crimes by major category", calculateCrimesByMajorCategory(contents))
      timed("Exporting resulting aggregation - by major category to csv", saveDataFrameToCsv(crimesByMajorCategory, buildFilePath(destinationFolder, "total_crimes_by_major_category.csv")))
      timed("Exporting resulting aggregation - by major category to parquet", saveDataFrameToParquet(crimesByMajorCategory, buildFilePath(destinationFolder, "total_crimes_by_major_category.parquet")))

      logger.info("Calculating total crimes by minor category")
      val crimesByMinorCategory = timed("Calculate total crimes by minor category", calculateCrimeCountByMinorCategory(contents))
      timed("Exporting resulting aggregation - by minor category to csv", saveDataFrameToCsv(crimesByMinorCategory, buildFilePath(destinationFolder, "total_crimes_by_minor_category.csv")))
      timed("Exporting resulting aggregation - by minor category to parquet", saveDataFrameToParquet(crimesByMinorCategory, buildFilePath(destinationFolder, "total_crimes_by_minor_category.parquet")))

      logger.info("Calculating total crimes by borough and year")
      val crimesByBoroughAndYear = timed("Calculate total crimes by borough and year", calculateCrimeCountByBoroughAndYear(contents))
      timed("Exporting resulting aggregation - by borough and year to csv", saveDataFrameToCsv(crimesByBoroughAndYear, buildFilePath(destinationFolder, "total_crimes_by_borough_year.csv")))
      timed("Exporting resulting aggregation - by borough and year to parquet", saveDataFrameToParquet(crimesByBoroughAndYear, buildFilePath(destinationFolder, "total_crimes_by_borough_year.parquet")))

      logger.info("Calculating total crimes by major category and year")
      val crimesByMajorCategoryAndYear = timed("Calculate total crimes by major category and year", calculateCrimesByMajorCategoryAndYear(contents))
      timed("Exporting resulting aggregation - by major category and year to csv", saveDataFrameToCsv(crimesByMajorCategoryAndYear, buildFilePath(destinationFolder, "total_crimes_by_major_category_year.csv")))
      timed("Exporting resulting aggregation - by major category and year to parquet", saveDataFrameToParquet(crimesByMajorCategoryAndYear, buildFilePath(destinationFolder, "total_crimes_by_major_category_year.parquet")))

      logger.info("Calculating total crimes by minor category and year")
      val crimesByMinorCategoryAndYear = timed("Calculate total crimes by minor category and year", calculateCrimesByMinorCategoryAndYear(contents))
      timed("Exporting resulting aggregation - by minor category and year to csv", saveDataFrameToCsv(crimesByMinorCategoryAndYear, buildFilePath(destinationFolder, "total_crimes_by_minor_category_year.csv")))
      timed("Exporting resulting aggregation - by minor category and year to parquet", saveDataFrameToParquet(crimesByMinorCategoryAndYear, buildFilePath(destinationFolder, "total_crimes_by_minor_category_year.parquet")))

      logger.info("Calculating total crimes by year")
      val crimesByYear = timed("Calculate total crimes by year", calculateCrimesByYear(contents))
      timed("Exporting crimes by year results to csv", saveDataFrameToCsv(crimesByYear, buildFilePath(destinationFolder, "total_crimes_by_year.csv")))
      timed("Exporting crimes by year results to parquet", saveDataFrameToParquet(crimesByYear, buildFilePath(destinationFolder, "total_crimes_by_year.parquet")))

      logger.info("Calculating total crimes by year and month")
      val crimesByYearMonth = timed("Calculate total crimes by year", calculateCrimesByYearAndMonth(contents))
      timed("Exporting crimes by year and month results to CSV", saveDataFrameToCsv(crimesByYearMonth, buildFilePath(destinationFolder, "total_crimes_by_year_month.csv")))
      timed("Exporting crimes by year and month results to parquet", saveDataFrameToParquet(crimesByYearMonth, buildFilePath(destinationFolder, "total_crimes_by_year_month.parquet")))

      logger.info("Percentages of crimes by years")
      val crimePercentageByYear = calculateCrimesPercentageByCategoryAndYear(contents, sparkSession)
      crimePercentageByYear.foreach(item => {
        logger.info(s"Exporting crime percentage for year '${item._1}'")
        timed(s"Exporting crime percentage in ${item._1} to CSV", saveDataFrameToCsv(item._2, buildFilePath(destinationFolder, s"crime_percentage_${item._1}.csv")))
        timed(s"Exporting crime percentage in ${item._1} to parquet", saveDataFrameToParquet(item._2, buildFilePath(destinationFolder, s"crime_percentage_${item._1}.parquet")))
      })

      logger.info("Calculating total crimes by year and LSOA codes")
      val totalCrimesByYearAndLsoa = timed("Calculating total crimes by year and lsoa codes", calculateTotalCrimesByYearLsoaCode(contents))
      timed("Exporting results to csv", saveDataFrameToCsv(totalCrimesByYearAndLsoa, buildFilePath(destinationFolder, "total_crimes_by_year_lsoa_code.csv")))
      timed("Exporting results to parquet", saveDataFrameToCsv(totalCrimesByYearAndLsoa, buildFilePath(destinationFolder, "total_crimes_by_year_lsoa_code.parquet")))
      println(timing)

      logger.info("Exiting Extract London Crime data information")
    } else {
      logger.error("Missing arguments.")
    }
  }

  private def buildFilePath(folder: String, fileName: String): String = {
    s"$folder" + getDefault.getSeparator + fileName
  }
}
