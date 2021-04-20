package org.hw3.otus

import java.util.Properties

import org.apache.spark.sql.functions.{avg, count, lit, max, min, sum}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.hw3.otus.structure.TaxiFacts

object DataSetTask3 {

  def main(args: Array[String]): Unit = {
    import ReadAndWrite._

    implicit val spark = SparkSession
      .builder()
      .appName("DataSet")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxiFactsDF: DataFrame = readParquet("src/main/data/yellow_taxi_jan_25_2018")
    val taxiFactsDS: Dataset[TaxiFacts] = taxiFactsDF.as[TaxiFacts]
    val resDataSet = getDataMart(taxiFactsDS).cache()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "docker")
    connectionProperties.put("password", "docker")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    val url = "jdbc:postgresql://127.0.0.1:5432/docker"

    writeDFToPostgres(resDataSet, url, "travel_by_taxi_info", connectionProperties)

    spark.catalog.clearCache()
  }

  def getDataMart(ds: Dataset[TaxiFacts]): DataFrame = {
    ds
      .filter(t => t.trip_distance > 0)
      .groupBy("VendorID")
      .agg(
        sum("total_amount").alias("sum_amount"),
        count(lit(1)).alias("count_orders"),
        sum("passenger_count").alias("sum_passengers"),
        avg("trip_distance").alias("avg_trip_distance"),
        min("trip_distance").alias("min_trip_distance"),
        max("trip_distance").alias("max_trip_distance"),
      )
      .orderBy("VendorID")
  }

}
