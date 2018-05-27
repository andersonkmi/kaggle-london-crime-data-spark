package org.sharpsw.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ExtractLondonCrimeData._

class ExtractLondonCrimeDataSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  def initializeExtractLondonCrimeData(): Boolean =
    try {
      ExtractLondonCrimeData
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeExtractLondonCrimeData(), " -- did you fill in all the values in WordCount (sc)?")
    //sparkSession.sparkContext.stop()
  }


  private def createDataFrame(): DataFrame = {
    val sampleData = Seq(
      Row("code001", "region 001", "major_category 001", "minor category 001/001", 1, 2001, 1),
      Row("code001", "region 001", "major_category 001", "minor category 001/002", 3, 2001, 1)
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

  "Extracting boroughs test case 1" should "return 1" in {
    val df = createDataFrame()
    val locations = extractDistinctBoroughs(df)
    val results = locations.collect().map(_(0)).toList
    results.size shouldEqual 1
  }

  "Extracting boroughs test case 2" should "equals region 001" in {
    val df = createDataFrame()
    val locations = extractDistinctBoroughs(df)
    val results = locations.collect().map(_(0)).toList
    results.head shouldEqual "region 001"
  }
}
