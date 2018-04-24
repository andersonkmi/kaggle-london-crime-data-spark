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
    val conf: SparkConf = new SparkConf().setAppName("Word count").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

  }
}
