package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.sharpsw.spark.utils.TraceUtil.{timed, timing}

object WordCount {
  val sparkSession: SparkSession = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileContents = sparkSession.sparkContext.textFile(args(0))
    val result = timed("Step 1 - Executing the counting process", countWords(fileContents))
    timed("Step 2 - Saving results", persist(result, args(1)))

    println(timing)
  }

  def countWords(contents: RDD[String]): RDD[(String, Int)] = {
    val words = contents.flatMap(_.split(" "))
    words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  }

  def persist(result: RDD[(String, Int)], destinationFolder: String): Unit = {
    result.saveAsTextFile(destinationFolder)
  }
}
