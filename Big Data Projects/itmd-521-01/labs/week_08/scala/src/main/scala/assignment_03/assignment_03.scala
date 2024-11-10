import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType}
import org.apache.spark.sql.functions.{col, month, dayofmonth}

object assignment_03 {
def main(args: Array[String]): Unit = {
    
    // Create SparkSession
    val spark = SparkSession.builder.appName("Flights_scala").getOrCreate()

    import spark.implicits._

    // Read file path dynamically from command line argument
    val departureDelays = args(0)

    // Read departuredelays.csv using DataFrameReader
    val df_1 = spark.read.option("header", "true").csv(departureDelays)

    // Register df as a temporary view with the name "us_delay_flights_tbl"
    df_1.createOrReplaceTempView("us_delay_flights_tbl")

    ///// Part I ////

    // Implement Spark SQL queries
    println("Query 1_sql Results:")
    spark.sql("""SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)

    println("Query 2_sql Results:")
    spark.sql("""SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC""").show(10)

    // Implement Spark DataFrame API queries equivalent to Spark SQL queries
    println("Query 1_api Results:")
    df_1.select("distance", "origin", "destination").where("distance > 1000").orderBy($"distance".desc).show(10)
    
    //println("Query 2_api Results:")
    df_1.select("date", "delay", "origin", "destination").filter(col("delay") > 120 && col("origin") === "SFO" && col("destination") === "ORD").orderBy(col("delay").desc).show(10)

    

    ///// Part II ////

    

    // Filter flights with origin ORD and month/day between 03/01 and 03/15
    val filtered_Ord_DF = spark.sql("SELECT * FROM us_delay_flights_tbl WHERE origin = 'ORD' AND date >= '0301' AND date <= '0315'")

    //Show the first 5 records of the filtered DataFrame
    filtered_Ord_DF.show(5)

    // Use Spark Catalog to list the columns of the table us_delay_flights_tbl
    println("Columns of table us_delay_flights_tbl:")
    spark.catalog.listColumns("us_delay_flights_tbl").foreach(column => println(column.name))

    

    ///// Part III ////

    
    // Read departuredelays.csv into a DataFrame and apply schema

    val flight_Schema = StructType(Seq(
    StructField("date", StringType, nullable = true),
    StructField("delay", IntegerType, nullable = true),
    StructField("distance", IntegerType, nullable = true),
    StructField("origin", StringType, nullable = true),
    StructField("destination", StringType, nullable = true)))

    // Read departuredelays.csv using DataFrameReader
    val df = spark.read.schema(flight_Schema).option("header", "true").csv(departureDelays)

    // Write DataFrame content as JSON
    df.write.mode("overwrite").json("scala_departuredelays.json")

    // Write DataFrame content as JSON with lz4 compression
    df.write.mode("overwrite").option("compression", "lz4").json("scala_departuredelays_lz4")

    // Write DataFrame content as Parquet
    df.write.mode("overwrite").parquet("scala_departuredelays.parquet")

    

    ///// Part IV ////

    
    // Write DataFrame content as Parquet
    df.write.mode("overwrite").parquet("scala_departuredelays.parquet")

    // Read departuredelays.parquet file into a DataFrame
    val departureDelays_DF = spark.read.parquet("scala_departuredelays.parquet")

    // Convert date column from 'MMddHHmm' to 'MM-dd HH:mm'
    val formatted_DF = departureDelays_DF
    .withColumn("timestamp", unix_timestamp($"date", "MMddHHmm"))   // Convert to UNIX timestamp
    .withColumn("date", from_unixtime($"timestamp", "MM-dd HH:mm")) // Convert to desired format
    .drop("timestamp")                                              // Drop the intermediate UNIX timestamp column

    // Select records where the origin airport is ORD
    val ord_DepartureDelays_DF = formatted_DF.filter($"origin" === "ORD")

    // Show the first 10 lines of the DataFrame
    ord_DepartureDelays_DF.show(10)

    
    
    // Stop SparkSession
    spark.stop()
}
}