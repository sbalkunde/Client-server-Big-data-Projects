from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F

# Create SparkSession
spark = SparkSession.builder.appName("Flights_py").getOrCreate()

# Read file path dynamically from command line argument
departuredelays = sys.argv[1]

# Read departuredelays.csv using DataFrameReader
df = spark.read.csv(departuredelays, header=True)

# Register df as a temporary view with the name "us_delay_flights_tbl"
df.createOrReplaceTempView("us_delay_flights_tbl")

##### Part I #####

# Implement Spark SQL queries from page 87

#print("Query 1_sql Results:")
spark.sql("""SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)

#print("Query 2_sql Results:")
spark.sql("""SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC""").show(10)


# Implement Spark DataFrame API queries equivalent to Spark SQL queries from page 87

print("Query 1_api Results:")
q1_df_api = df.select("distance", "origin", "destination").filter(col("distance") > 1000).orderBy(col("distance").desc()).show(10)

print("Query 2_api Results:")
q2_df_api = df.select("date", "delay", "origin", "destination").filter((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")).orderBy(col("delay").desc()).show(10)

##### Part II #####

# Filter flights with origin ORD and month/day between 03/01 and 03/15
filtered_ord_df = df.filter((col("origin") == "ORD") & (col("date").between("0301", "0315")))

# Show the first 5 records of the filtered DataFrame
filtered_ord_df.show(5)

# Use Spark Catalog to list the columns of the table us_delay_flights_tbl
columns = spark.catalog.listColumns("us_delay_flights_tbl")
print("Columns of table us_delay_flights_tbl:")
for column in columns:print(column.name)

##### Part III #####

# Read departuredelays.csv into a DataFrame and apply schema

flight_schema = StructType([
    StructField("date", StringType(), True),
    StructField("delay", IntegerType(), True),
    StructField("distance", IntegerType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True)]) 
    

# Read file path dynamically from command line argument
departuredelays = sys.argv[1]

# Read departuredelays.csv using DataFrameReader
df = spark.read.csv(departuredelays, schema=flight_schema, header=True)

# Write DataFrame content as JSON
df.write.mode("overwrite").json("departuredelays.json")

# Write DataFrame content as JSON with lz4 compression 
df.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4")

# Write DataFrame content as Parquet
df.write.mode("overwrite").parquet("departuredelays.parquet")

##### Part IV #####

# Write DataFrame content as Parquet
df.write.mode("overwrite").parquet("departuredelays.parquet")

# Read departuredelays.parquet file into a DataFrame
departuredelays_df = spark.read.parquet("departuredelays.parquet")

# Convert 'date' from 'MMddHHmm' to a UNIX timestamp, then to 'MM-dd HH:mm' format, and then drop the temporary column
departuredelays_df = departuredelays_df.withColumn("date", F.from_unixtime(F.unix_timestamp(F.col("date"), "MMddHHmm"), "MM-dd HH:mm"))

# Select records where the origin airport is ORD
ord_departuredelays_df = departuredelays_df.filter(departuredelays_df.origin == "ORD")

# Show the first 10 lines of the DataFrame
ord_departuredelays_df.show(10)


# Stop SparkSession
spark.stop()