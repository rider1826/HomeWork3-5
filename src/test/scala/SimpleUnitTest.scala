import org.apache.spark.sql.SparkSession
import org.hw3.otus.ReadAndWrite._
import org.hw3.otus.structure.TaxiFacts
import org.hw3.otus.RDDTask2.getMostPopularTimeOrders
import org.scalatest.flatspec.AnyFlatSpec

class SimpleUnitTest extends AnyFlatSpec{

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test for RDD")
    .getOrCreate()

  import spark.implicits._

  it should "upload and process data" in {
    val checkedDF = readParquet("src/main/data/yellow_taxi_jan_25_2018")

    val actualDistribution = getMostPopularTimeOrders(checkedDF.as[TaxiFacts].rdd)

    assert(actualDistribution === Array(("2018-01-25 09:51:56", 18)))
  }

}
