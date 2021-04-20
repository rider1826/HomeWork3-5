import org.hw3.otus.DataFrameTask1.getMostPopularDistricts
import org.hw3.otus.DataSetTask3.getDataMart
import org.hw3.otus.ReadAndWrite._
import org.hw3.otus.structure.TaxiFacts
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestSharedSparkSession extends SharedSparkSession{
  import testImplicits._

  test("Test for Dataset") {
    val taxiFactsDF = readParquet("src/main/data/yellow_taxi_jan_25_2018")
    val taxiFactsDS: Dataset[TaxiFacts] = taxiFactsDF.as[TaxiFacts]

    val actualDistribution = getDataMart(taxiFactsDS)//.

    actualDistribution.collect().foreach { c =>
      val vendorId = c.get(0).asInstanceOf[Int]
      val sumAmount = c.get(1).asInstanceOf[Double]
      val countOrders = c.get(2).asInstanceOf[Long]
      val sumPassengers = c.get(3).asInstanceOf[Long]
      val avgTripDistance = c.get(4).asInstanceOf[Double]
      val minTripDistance = c.get(5).asInstanceOf[Double]
      val maxTripDistance = c.get(6).asInstanceOf[Double]

      (vendorId  != null ) shouldBe(true)
      (sumAmount > 0) shouldBe(true)
      (countOrders > 0) shouldBe(true)
      (sumPassengers > 0) shouldBe(true)
      (minTripDistance > 0) shouldBe(true)
      (minTripDistance <= avgTripDistance) shouldBe(true)
      (avgTripDistance <= maxTripDistance) shouldBe(true)
    }

    checkAnswer(actualDistribution,
      Row(1,2274108.680001364,146453,171162,2.6204461499593914,0.1,66) ::
      Row(2,2958166.989997808,183570,350248,2.8234976848068913,0.01,55.41) :: Nil)
  }

  test("Test for DataFrame") {
    val taxiZoneDF = readCSV("src/main/data/taxi_zones.csv")
    val taxiFactsDF = readParquet("src/main/data/yellow_taxi_jan_25_2018")

    val actualDistribution = getMostPopularDistricts(taxiFactsDF, taxiZoneDF)

    checkAnswer(
      actualDistribution,
      Row("Manhattan",296527)  ::
        Row("Queens",13819) ::
        Row("Brooklyn",12672) ::
        Row("Unknown",6714) ::
        Row("Bronx",1589) ::
        Row("EWR",508) ::
        Row("Staten Island",64) :: Nil
    )
  }
}
