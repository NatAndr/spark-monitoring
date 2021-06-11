package org.example

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.example.Utils.{readCsv, readParquet}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs
import org.apache.hadoop.fs._

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
    val logger = LoggerFactory.getLogger(this.getClass.getName)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("spark_app")
      .config("spark.metrics.namespace", "spark_app")
      .getOrCreate()

    logger.info("Start processing data")

    val taxiDF = readParquet("hdfs:///data/yellow_taxi_jan_25_2018")
    val taxiZonesDF = readCsv("hdfs:///data/taxi_zones.csv")

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
