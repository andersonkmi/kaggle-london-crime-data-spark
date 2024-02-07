package org.sharpsw.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import LondonCrimeDataExplorer._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtractLondonCrimeDataSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder.appName("ExtractLondonCrimeData").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  private def createDataFrame(): DataFrame = {
    val sampleData = Seq(
      Row("code001", "region 001", "major_category 001", "minor category 001/001", 1, 2001, 1),
      Row("code001", "region 001", "major_category 001", "minor category 001/002", 3, 2001, 1),
      Row("code002", "region 002", "major_category 001", "minor category 001/001", 3, 2001, 1)
    )

    val schema = schemaDefinition()
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(sampleData), schema)
  }

  private def schemaDefinition(): StructType = {
    val lsoaCodeField       = StructField("lsoa_code", StringType, nullable = false)
    val boroughField        = StructField("borough", StringType, nullable = false)
    val majorCategoryField  = StructField("major_category", StringType, nullable = false)
    val minorCategoryField  = StructField("minor_category", StringType, nullable = false)
    val valueField          = StructField("value", IntegerType, nullable = false)
    val yearField           = StructField("year", IntegerType, nullable = false)
    val monthField          = StructField("month", IntegerType, nullable = false)

    val fields = List(lsoaCodeField, boroughField, majorCategoryField, minorCategoryField, valueField, yearField, monthField)
    StructType(fields)
  }

  "Extracting boroughs test case 1" should "return 2" in {
    val df = createDataFrame()
    val locations = extractDistinctBoroughs(df)
    val results = locations.collect().map(_(0)).toList
    results.size shouldEqual 2
  }

  "Extracting boroughs test case 2" should "contain region 001 and region 002" in {
    val df = createDataFrame()
    val locations = extractDistinctBoroughs(df)
    val results = locations.collect().map(_(0)).toList
    results.head shouldEqual "region 001"
    results(1) shouldEqual "region 002"
  }

  "Extracting major categories test case 001" should "return 1" in {
    val df = createDataFrame()
    val items = extractDistinctMajorCrimeCategories(df)
    val results = items.collect().map(_(0)).toList
    results.size shouldEqual 1
  }

  "Extracting major categories contents" should "return major_category 001" in {
    val df = createDataFrame()
    val items = extractDistinctMajorCrimeCategories(df)
    val results = items.collect().map(_(0)).toList
    results.head shouldEqual "major_category 001"
  }

  "Extracting minor categories count" should "return 2" in {
    val df = createDataFrame()
    val items = extractDistinctMinorCrimeCategories(df)
    val results = items.collect().map(_(0)).toList
    results.size shouldEqual 2
  }

  "Extracting minor categories contents" should "contain minor category 001/001 and minor category 001/002" in {
    val df = createDataFrame()
    val items = extractDistinctMinorCrimeCategories(df)
    val results = items.collect().map(_(0)).toList
    results.head shouldEqual "minor category 001/001"
    results(1) shouldEqual "minor category 001/002"
  }

  "Extracting combined categories count" should "return 2" in {
    val df = createDataFrame()
    val items = extractCombinedCategories(df)
    items.count() shouldEqual 2
  }

  "Extracting combined categories contents" should "contain major_category 001,minor category 001/001 and major_category 001,minor category 001/002" in {
    val df = createDataFrame()
    val items = extractCombinedCategories(df)
    val results = items.map(item => (item.getString(0), item.getString(1))).collect().toList

    results.head._1 shouldEqual "major_category 001"
    results.head._2 shouldEqual "minor category 001/001"

    results(1)._1 shouldEqual "major_category 001"
    results(1)._2 shouldEqual "minor category 001/002"
  }

  "Counting total crimes by borough" should "return region 001 / 4" in {
    val df = createDataFrame()
    val items = calculateTotalCrimeCountByBorough(df)
    val results = items.map(item => (item.getString(0), item.getLong(1))).collect().toList

    results.head._1 shouldEqual "region 001"
    results.head._2 shouldEqual 4
  }

  "Counting total crimes by major category" should "return major_category 001 / 7" in {
    val df = createDataFrame()
    val items = calculateCrimesByMajorCategory(df)
    val results = items.map(item => (item.getString(0), item.getLong(1))).collect().toList

    results.head._1 shouldEqual "major_category 001"
    results.head._2 shouldEqual 7
  }

  "Counting total crimes by minor category" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimeCountByMinorCategory(df)
    val results = items.map(item => (item.getString(0), item.getString(1), item.getLong(2))).collect().toList

    results.head._1 shouldEqual "major_category 001"
    results.head._2 shouldEqual "minor category 001/001"
    results.head._3 shouldEqual 4

    results(1)._1 shouldEqual "major_category 001"
    results(1)._2 shouldEqual "minor category 001/002"
    results(1)._3 shouldEqual 3
  }

  "Counting total crimes by borough and year" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimeCountByBoroughAndYear(df)
    val results = items.map(item => (item.getString(0), item.getInt(1), item.getLong(2))).collect().toList

    results.size shouldEqual 2

    results.head._1 shouldEqual "region 001"
    results.head._2 shouldEqual 2001
    results.head._3 shouldEqual 4

    results(1)._1 shouldEqual "region 002"
    results(1)._2 shouldEqual 2001
    results(1)._3 shouldEqual 3
  }

  "Counting total crimes by major category and year" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimesByMajorCategoryAndYear(df)
    val results = items.map(item => (item.getString(0), item.getInt(1), item.getLong(2))).collect().toList

    results.size shouldEqual 1

    results.head._1 shouldEqual "major_category 001"
    results.head._2 shouldEqual 2001
    results.head._3 shouldEqual 7
  }

  "Counting total crimes by minor category and year" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimesByMinorCategoryAndYear(df)
    val results = items.map(item => (item.getString(0), item.getString(1), item.getInt(2), item.getLong(3))).collect().toList

    results.size shouldEqual 2

    results.head._1 shouldEqual "major_category 001"
    results.head._2 shouldEqual "minor category 001/001"
    results.head._3 shouldEqual 2001
    results.head._4 shouldEqual 4

    results(1)._1 shouldEqual "major_category 001"
    results(1)._2 shouldEqual "minor category 001/002"
    results(1)._3 shouldEqual 2001
    results(1)._4 shouldEqual 3
  }

  "Counting total crimes by year" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimesByYear(df)
    val results = items.map(item => (item.getInt(0), item.getLong(1))).collect().toList

    results.size shouldEqual 1

    results.head._1 shouldEqual 2001
    results.head._2 shouldEqual 7
  }

  "Counting total crimes by year and month" should "be OK" in {
    val df = createDataFrame()
    val items = calculateCrimesByYearAndMonth(df)
    val results = items.map(item => (item.getInt(0), item.getInt(1), item.getLong(2))).collect().toList

    results.size shouldEqual 1

    results.head._1 shouldEqual 2001
    results.head._2 shouldEqual 1
    results.head._3 shouldEqual 7
  }
}
