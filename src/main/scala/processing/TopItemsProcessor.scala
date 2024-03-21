package htx.nigel.application
package processing
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
object TopItemsProcessor {
  def findTopXItems(spark: SparkSession,
                    detectionPath: String,
                    locationPath: String,
                    outputPath: String,
                    topX: Int): Unit = {
    // Converting detections parquet into an RDD, and casting column data types
    val detectionsRDD = spark.read.parquet(detectionPath).rdd.map(row =>
      (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name"))
    )

    // Converting locations parquet into an RDD, and casting column data types
    val locationsRDD = spark.read.parquet(locationPath).rdd.map(row =>
      (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location"))
    ).collectAsMap()

    // Distributing the Map of locationsRDD map to Spark Nodes
    val broadcastLocations = spark.sparkContext.broadcast(locationsRDD)

    // Join using broadcast hash join without explicitly using .join(). detectionsRDD attempts to find a matching key in broadcasted locationsRDD using .get()
    val joinedRDD = detectionsRDD.flatMap { case (locId, itemName) =>
      broadcastLocations.value.get(locId).map(location => ((location, itemName), 1)) // if a match is found, a new tuple is constructed with location and item_name, and a count of "1"
    }

    // Reduce function by key to count occurrences of each item within each location
    val itemCountsRDD = joinedRDD.reduceByKey(_ + _) // something like (a, b) => a+b in javascript

    // Prepare for ranking: ((location, itemName), count) => (location, (itemName, count))
    val itemsWithLocationAsKey = itemCountsRDD.map { case ((location, itemName), count) =>
      (location, (itemName, count))
    }

    // Group by location key first, and sort by count within each group to find top items
    val groupedAndRanked = itemsWithLocationAsKey.groupByKey().flatMap { case (location, items) =>
      val topItems = items.toList.sortBy(-_._2).take(topX) // converts iterable items into list, sort list in descending order, takes topX items in this list. _._2 refers to the count part of the (itemName, count) tuple
      topItems.zipWithIndex.map { case ((itemName, count), index) =>
        (location, itemName, index + 1) // Convert 0-based index to rank that starts with 1.
      }
    }

    // Convert to rowRDD for DataFrame creation
    val rowRDD = groupedAndRanked.map { case (location, itemName, rank) =>
      Row(location, itemName, rank)
    }

    // Define the schema for DataFrame
    val schema = StructType(Array(
      StructField("geographical_location", StringType, nullable = true),
      StructField("item_name", StringType, nullable = true),
      StructField("item_rank", IntegerType, nullable = true)
    ))

    // Convert RDD[Row] to DataFrame
    val finalDF = spark.createDataFrame(rowRDD, schema)

    // Write the DataFrame as a Parquet file
    finalDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
