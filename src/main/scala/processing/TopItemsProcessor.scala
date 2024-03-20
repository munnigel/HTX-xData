package htx.nigel.application
package processing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TopItemsProcessor {
  def findTopXItems(spark: SparkSession,
                    detectionPath: String,
                    locationPath: String,
                    outputPath: String,
                    topX: Int): Unit = {

    // Read the detection data Parquet file
    val detectionsDF = spark.read.parquet(detectionPath)

    // Read the geographical locations Parquet file
    val locationsDF = spark.read.parquet(locationPath)

    // Count the occurrences of each item per geographical location
    val itemCountsDF = detectionsDF
      .groupBy("geographical_location_oid", "item_name")
      .agg(count("item_name").alias("item_count"))

    // Join the counts with the geographical locations on the location OID
    val joinedDF = itemCountsDF
      .join(locationsDF, Seq("geographical_location_oid"))
      .select("geographical_location", "item_name", "item_count")

    // Window specification to rank items by count within each geographical location
    val windowSpec = Window.partitionBy("geographical_location").orderBy(col("item_count").desc)

    // Apply the window specification to calculate ranks
    val rankedDF = joinedDF
      .withColumn("item_rank", dense_rank().over(windowSpec))
      .orderBy("geographical_location", "item_rank", "item_name")

    // Filter to limit to topX ranks
    val topXRankedDF = rankedDF.filter(col("item_rank") <= topX)

    // Remove 'item_count'
    val finalDF = topXRankedDF.drop("item_count")

    // Write the result to the output Parquet file
    finalDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
