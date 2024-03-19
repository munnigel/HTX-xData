package htx.nigel.application.processing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TopItemsProcessorSkewed {
  def findTopXItems(spark: SparkSession,
                    detectionPath: String,
                    locationPath: String,
                    outputPath: String,
                    topX: Int): Unit = {

    // Enable AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    // Read the detection data Parquet file
    val detectionsDF = spark.read.parquet(detectionPath)

    // Add a salt to the 'geographical_location_oid' to address skew
    val saltedDetectionsDF = detectionsDF
      .withColumn("salt", (rand() * 4).cast("int"))
      .withColumn("salted_oid", concat(col("geographical_location_oid"), lit("_"), col("salt")))

    // Read the geographical locations Parquet file
    val locationsDF = spark.read.parquet(locationPath)

    // Explode locationsDF to match the salt range in saltedDetectionsDF
    val explodedLocationsDF = locationsDF
      .withColumn("salt", explode(array((0 until 4).map(lit): _*)))
      .withColumn("salted_oid", concat(col("geographical_location_oid"), lit("_"), col("salt")))
      .drop("salt")

    // Join using the salted_oid
    val joinedDF = saltedDetectionsDF
      .join(broadcast(explodedLocationsDF), "salted_oid")
      .drop("salted_oid", "salt")

    // Now perform the aggregation on joinedDF to get counts by item and location
    val aggregatedDF = joinedDF
      .groupBy("geographical_location", "item_name")
      .agg(count("item_name").alias("item_count"))

    // Apply the window specification to calculate ranks within each geographical location
    val windowSpec = Window.partitionBy("geographical_location").orderBy(col("item_count").desc)

    val rankedDF = aggregatedDF
      .withColumn("item_rank", dense_rank().over(windowSpec))
      .where(col("item_rank") <= topX)
      .orderBy("geographical_location", "item_rank", "item_name")

    // Write the result to the output Parquet file
    rankedDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
