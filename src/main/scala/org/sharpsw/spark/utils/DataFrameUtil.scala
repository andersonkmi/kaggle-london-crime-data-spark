package org.sharpsw.spark.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.asc

object DataFrameUtil {
  def extractDistinctValues(contents: DataFrame, columnName: String): DataFrame = {
    contents.select(contents(columnName)).distinct.orderBy(asc(columnName))
  }

  def saveDataFrameToCsv(contents: DataFrame, dest: String): Unit = {
    contents.coalesce(1).write.mode("overwrite").option("header", "true").csv(dest)
  }

  def saveDataFrameToParquet(df: DataFrame, dest: String): Unit = {
    df.coalesce(1).write.mode("overwrite").parquet(dest)
  }

}
