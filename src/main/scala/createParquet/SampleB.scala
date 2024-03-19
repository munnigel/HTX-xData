package htx.nigel.application
package createParquet

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
object SampleB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sample B Creator")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("geographical_location", StringType, nullable = false)
    ))

    // Repetitive data for geographical location data
    val locationData = Seq(
      Row(1L, "Downtown"),
      Row(2L, "Uptown"),
      Row(3L, "Suburbs"),
    )

    val rdd = spark.sparkContext.parallelize(locationData)
    val sampleDataFrame = spark.createDataFrame(rdd, schema)

    sampleDataFrame.write.mode(SaveMode.Overwrite).parquet("src/main/resources/sampleB.parquet")
    sampleDataFrame.show(truncate = false)

    spark.stop()
  }
}

