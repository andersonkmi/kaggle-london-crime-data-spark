package org.sharpsw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }


  def main(args: Array[String]): Unit = {
    println("Args length: " + args.length)

    val sc = timed("Step 1 - Initializing Spark context", initSpark())
    val result = timed("Step 2 - Executing the counting process", process(args(0), sc))
    timed("Step 3 - Saving results", persist(result, args(1)))

    println(timing)
    sc.stop()
  }

  private def initSpark(master: String = "local[*]"): SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("Word count").setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    sc
  }

  private def process(input: String, sc: SparkContext): RDD[(String, Int)] = {
    val contents = sc.textFile(input)
    val words = contents.flatMap(_.split(" "))
    words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  }

  private def persist(result: RDD[(String, Int)], destinationFolder: String): Unit = {
    result.saveAsTextFile(destinationFolder)
  }
}
