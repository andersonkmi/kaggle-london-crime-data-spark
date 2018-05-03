package org.sharpsw.spark

import org.apache.spark.{SparkConf, SparkContext}

object TextFileLoader {
  val sc:SparkContext = new SparkContext(new SparkConf().setAppName("TextFileLoader").setMaster("local[*]"))

  def main(args: Array[String]): Unit = {
    val fileContents = sc.textFile(args(0))
    println(fileContents)
  }
}
