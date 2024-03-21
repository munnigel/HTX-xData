package htx.nigel.application
package processing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object TopItemsProcessorSkewed {
  def findTopXItems(spark: SparkSession,
                    detectionPath: String,
                    locationPath: String,
                    outputPath: String,
                    topX: Int): Unit = {
    import spark.implicits._

    // Read the detection data Parquet file, convert to RDD, and introduce salting to the keys
    val saltedDetectionsRDD = spark.read.parquet(detectionPath).rdd.flatMap(row => {
      val locationOid = row.getAs[Long]("geographical_location_oid")
      val itemName = row.getAs[String]("item_name")
      // Generate multiple salted keys (0 to 4) for each record to distribute skew
      (0 until 5).map(salt => ((locationOid.toString + "_" + salt), itemName))
    })

    // Read the geographical locations Parquet file and convert to a Map
    val locationsMap = spark.read.parquet(locationPath).rdd.flatMap(row => {
      val locationOid = row.getAs[Long]("geographical_location_oid").toString
      val locationName = row.getAs[String]("geographical_location")
      // Replicate location entries for each salt value (0 to 4) used in detections
      (0 until 5).map(salt => ((locationOid + "_" + salt), locationName))
    }).collectAsMap()

    // Distributing the Map of locationsRDD map to Spark Nodes
    val broadcastLocations = spark.sparkContext.broadcast(locationsMap)

    // Perform a join with the broadcasted locations, without using .join. .get() is used to find the matching locationID.
    val joinedRDD = saltedDetectionsRDD.mapPartitions(iter => {
      val locMap = broadcastLocations.value
      iter.flatMap {
        case (saltedLocId, itemName) =>
          locMap.get(saltedLocId).map(location => (((location, itemName), 1))) // finds the same locId, maps each location and item name to a count of 1
      }
    }).reduceByKey(_ + _) // sums count for each (location, itemName) pair of key, incrementing by 1

    // Transformations to rank items within each location
    val rankedItemsRDD = joinedRDD.map {
      case ((location, itemName), count) => (location, (itemName, count)) // groups data based on location
    }.groupByKey().flatMap {
      case (location, items) =>
        items.toList.sortBy(-_._2).zipWithIndex.map {
          case ((itemName, count), index) => (location, itemName, index + 1) // sort items by count in descending order
        }.take(topX) // take the specified topX
    }

    // Convert RDD to DataFrame for saving
    val finalDF = spark.createDataFrame(rankedItemsRDD.map(Row.fromTuple), new StructType()
      .add("geographical_location", StringType, nullable = true)
      .add("item_name", StringType, nullable = true)
      .add("item_rank", IntegerType, nullable = true))

    finalDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
