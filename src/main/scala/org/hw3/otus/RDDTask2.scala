package org.hw3.otus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import structure.TaxiFacts

object RDDTask2 {
  def main(args: Array[String]): Unit = {
    import ReadAndWrite._

    implicit val spark = SparkSession
      .builder()
      .appName("RDD")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxiFactsDF: DataFrame = readParquet("src/main/data/yellow_taxi_jan_25_2018")
    val taxiFactsDS: Dataset[TaxiFacts] = taxiFactsDF.as[TaxiFacts]
    val taxiFactsRDD: RDD[TaxiFacts] = taxiFactsDS.rdd

    val resRDD = getMostPopularTimeOrders(taxiFactsRDD)
    resRDD.foreach(println)
    writeRDDToTxtFile(resRDD, "src/main/data/resRDD.txt")

    spark.catalog.clearCache()
  }

  def getMostPopularTimeOrders(rdd: RDD[TaxiFacts]) = {
    rdd
      .map(t => (t.tpep_pickup_datetime, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .collect()
      .take(1)
  }
}
