package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read.load(path)

  def readCsv(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
}
