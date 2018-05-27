package org.sharpsw.spark

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

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
    ExtractLondonCrimeData.sparkSession.sparkContext.stop()
  }

}
