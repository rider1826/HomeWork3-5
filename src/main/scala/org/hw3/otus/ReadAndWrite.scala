package org.hw3.otus

import java.io.File
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.hw3.otus.structure.TaxiFacts

object ReadAndWrite {

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def writeDFToParquetFile(dataFrame: DataFrame, path: String, partitionCount: Int): Unit ={
    dataFrame
      .coalesce(partitionCount)
      .write
      .parquet(path)
  }

  def writeRDDToTxtFile (rdd: Array[(String, Int)], path: String)= {
    def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
      val p = new java.io.PrintWriter(f)
      try {
        op(p)
      } finally {
        p.close()
      }
    }

    printToFile(new File(path)) { p =>
      rdd.foreach(p.println)
    }
  }

  def writeDFToPostgres(ds: DataFrame, url: String, tableName: String, connectionProperties: Properties) = {
    ds
      .write
      .mode(SaveMode.Append)
      .jdbc(url, tableName, connectionProperties)
  }

}
