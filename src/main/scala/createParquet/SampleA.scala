package htx.nigel.application
package createParquet

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scala.util.Random

object SampleA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet A Creator")
      .master("local[*]") // Use all local cores
      .getOrCreate()

    // Define the schema
    val schema = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("video_camera_oid", LongType, nullable = false),
      StructField("detection_oid", LongType, nullable = false),
      StructField("item_name", StringType, nullable = false),
      StructField("timestamp_detected", LongType, nullable = false)
    ))
    // Random data generator for detection data
    val random = new Random()
    val minId = 1
    val maxId = 3
    val items = Seq("Person", "Car", "Bicycle", "Sign", "Tree")
    val sampleData = (1 to 100).map { i =>
      Row((minId + random.nextInt(maxId)).toLong, // Randomly assigns a value from 1 to 5
        (100 + i).toLong,
        (1000 + i).toLong,
        items(random.nextInt(items.length)),
        1622554472000L + i * 1000)
    }

    // Convert the sample data into a DataFrame and output as Parquet file
    val rdd = spark.sparkContext.parallelize(sampleData)
    val sampleDataFrame = spark.createDataFrame(rdd, schema)
    val outputDir = "src/main/resources"
    val outputPath = s"$outputDir/sampleA.parquet"

    val outputDirPath = new java.io.File(outputDir)
    if (!outputDirPath.exists()) {outputDirPath.mkdirs()}
    sampleDataFrame.write.mode(SaveMode.Overwrite).parquet(outputPath)
    sampleDataFrame.show(truncate = false)
    spark.stop()
  }
}

