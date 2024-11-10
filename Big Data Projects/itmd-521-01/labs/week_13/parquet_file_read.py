# Start your Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Parquet File Read").getOrCreate()

# Read the Parquet file
parquet_file = spark.read.parquet("s3a://sbalkunde/part-three.parquet")

# Show the contents of the Parquet file
parquet_file.show()

# Stop the Spark session
spark.stop()
