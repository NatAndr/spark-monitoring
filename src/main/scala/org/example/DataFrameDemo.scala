package org.example

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.example.Utils.{readCsv, readParquet}
import org.slf4j.LoggerFactory

object DataFrameDemo {

  def processTaxiData(taxiDF: DataFrame, taxiZonesDF: DataFrame)(implicit
                                                                 spark: SparkSession
  ): Dataset[Row] = {
    import spark.implicits._
    taxiDF
      .join(broadcast(taxiZonesDF), $"DOLocationID" === $"LocationID", "left")
      .groupBy($"Borough")
      .count()
      .orderBy($"count".desc)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("spark_app")
      .config("spark.metrics.namespace", "spark_app")
      .getOrCreate()

    val logger = LoggerFactory.getLogger(this.getClass.getName)
    logger.info("Start processing data 1")

    val taxiDF = readParquet("./yellow_taxi_jan_25_2018")
    val taxiZonesDF = readCsv("./taxi_zones.csv")

    val value = processTaxiData(taxiDF, taxiZonesDF)
    value.show()

    value
      .repartition(1)
      .write
      .parquet("topBoroughs.parquet")

    logger.info("End processing data")

    spark.stop()
  }

}
