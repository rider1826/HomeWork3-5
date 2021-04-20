package org.hw3.otus

object structure {
  case class TaxiFacts( VendorID: Int,
                        tpep_pickup_datetime: String ,
                        tpep_dropoff_datetime: String ,
                        passenger_count: Int,
                        trip_distance: Double,
                        RatecodeID: Int,
                        store_and_fwd_flag: String ,
                        PULocationID: Int,
                        DOLocationID: Int,
                        payment_type: Int,
                        fare_amount: Double,
                        extra: Double,
                        mta_tax: Double,
                        tip_amount: Double,
                        tolls_amount: Double,
                        improvement_surcharge: Double,
                        total_amount: Double)
}
