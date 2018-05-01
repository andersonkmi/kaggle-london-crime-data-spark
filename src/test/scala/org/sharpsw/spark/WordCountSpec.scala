package org.sharpsw.spark

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import WordCount._

class WordCountSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  def initializeWordCount(): Boolean =
    try {
      WordCount
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeWordCount(), " -- did you fill in all the values in WordCount (sc)?")
    sc.stop()
  }


  "The WordCount.countWords(empty)" should "return empty RDD" in {
    val emptyRDD = sc.parallelize(List(""))
    val result = countWords(emptyRDD)
    result.isEmpty() shouldEqual false
  }

  "The WordCount.countWords() with single value" should "return RDD OK" in {
    val input = sc.parallelize(List("anderson"))
    val result = countWords(input).collect()
    result.head._1 shouldEqual "anderson"
    result.head._2 shouldEqual 1
  }

  "The WordCount.countWords() with multiple values" should "return OK" in {
    val input = sc.parallelize(List("anderson", "ito", "spark", "scala", "scala"))
    val result = countWords(input).collect()
    result.size shouldEqual 4

    val res01 = result.find(item => item._1 == "anderson")
    res01.get._2 shouldEqual 1

    val res02 = result.find(item => item._1 == "scala")
    res02.get._2 shouldEqual 2

    val res03 = result.find(item => item._1 == "spark")
    res03.get._2 shouldEqual 1
  }
}
