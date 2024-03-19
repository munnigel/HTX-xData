package htx.nigel.application

import processing.TopItemsProcessor
import processing.TopItemsProcessorSkewed
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Main {
  private val log = Logger.getLogger(Main.getClass)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top X Detected Items")
      .master("local[*]")

      .getOrCreate()

    // Here you can pass the paths and topX as command line arguments
    // If running from an IDE, replace args(n) with actual string values for testing
    val detectionPath = "src/main/resources/sampleA.parquet" // flexibility to change to any input path
    val locationPath = "src/main/resources/sampleB.parquet" // flexibility to change to any input path
    val outputPath = "src/main/resources/output_top_items.parquet" // flexibility to change to any output path
    val topX = 3 // flexibility to change any top number

//    TopItemsProcessor.findTopXItems(spark, detectionPath, locationPath, outputPath, topX)
    TopItemsProcessorSkewed.findTopXItems(spark, detectionPath, locationPath, outputPath, topX)
    readOutputParquet()
    spark.stop()
  }

  private def readOutputParquet(): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Output Parquet")
      .master("local[*]")
      .getOrCreate()

    // Replace this with the actual path to your output Parquet file
    val outputPath = "src/main/resources/output_top_items.parquet"

    // Read the Parquet file
    val outputDataFrame = spark.read.parquet(outputPath)

    // Show the schema
    log.info("Schema of the output Parquet file:")
    outputDataFrame.printSchema()

    // Show the data
    log.info("Data contained in the output Parquet file:")
    outputDataFrame.show(truncate = false)

    spark.stop()
  }
}


