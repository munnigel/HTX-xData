package htx.nigel.application

import htx.nigel.application.processing.TopItemsProcessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll


class IntegrationTest extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("IntegrationTest")
      .master("local[*]")
      .getOrCreate()
  }

  test("TopItemsProcessor processes data correctly") {
    // Set the paths for input and output
    val detectionPath = "src/main/resources/sampleA.parquet"
    val locationPath = "src/main/resources/sampleB.parquet"
    val outputPath = "target/test/output/output_top_items.parquet"

    // Ensure the output directory exists
    new java.io.File(outputPath).getParentFile.mkdirs()

    // Run the processing
    TopItemsProcessor.findTopXItems(spark, detectionPath, locationPath, outputPath, 3)

    // Read the output
    val outputDF = spark.read.parquet(outputPath)

    // Assertions about the output data
    // For example, verify the count of items, schema, or specific data points
    assert(outputDF.count() > 0, "Output DataFrame should not be empty")
    assert(outputDF.columns.contains("geographical_location"), "Output should contain 'geographical_location' column")
    assert(outputDF.columns.contains("item_name"), "Output should contain 'item_name' column")
    assert(outputDF.columns.contains("item_rank"), "Output should contain 'item_rank' column")

    // More detailed checks can be added based on expected results
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}

