package org.sharpsw.spark

import org.apache.spark.sql.SparkSession

object TextFileLoader {
  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("TextFileLoader").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    println(fileContents)
  }
}
