package org.hw3.otus

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

object DataFrameTask1 {

  def main (args: Array[String]): Unit = {
    import ReadAndWrite._

    implicit val spark = SparkSession
      .builder()
      .appName("DataFrame")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiFactsDF: DataFrame = readParquet("src/main/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF: DataFrame = readCSV("src/main/data/taxi_zones.csv")
    val resDF: DataFrame = getMostPopularDistricts(taxiFactsDF, taxiZonesDF)

    resDF.show()
    writeDFToParquetFile(resDF, "src/main/data/resDF", 1)

    spark.catalog.clearCache()
  }

  def getMostPopularDistricts(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame = {
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
  }


}
