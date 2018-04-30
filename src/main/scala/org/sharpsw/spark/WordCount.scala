package org.sharpsw.spark

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
    val conf: SparkConf = new SparkConf().setAppName("Word count").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile(args(0))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
    counts.repartition(1).saveAsTextFile(args(1))

    sc.stop()
  }
}
