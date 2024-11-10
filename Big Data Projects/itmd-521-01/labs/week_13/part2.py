from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, year, month, avg, col

# Removing hard coded password - using os module to import them
import os
import sys

# Creating a Spark configuration object
conf = SparkConf()

#Setting JAR Packages-This package contains classes necessary for Spark to interact with AWS services
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')

#Setting AWS Credentials
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))

# S3 Configuration Settings
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')

# Internal IP for S3 cluster proxy
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

# Creating Spark Session
spark = SparkSession.builder.appName("part2 sbalkunde convert 60.txt to csv").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

# Reading Data from S3
df = spark.read.csv('s3a://itmd521/60.txt')

# cleaning & structing raw data in splitDF by splitting values in column '_c0' into multiple columns based on predefined positions and converting them to appropriate data types
splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
.withColumn('WBAN', df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

# print splitDF schema and showing top 5 rows
splitDF.printSchema()
splitDF.show(5)

# Create a CSV file of the cleaned data - Writing 60.txt data to a CSV file with no compression (uncompressed)
splitDF.write.format("csv").mode("overwrite").option("header", "true").save("s3a://sbalkunde/60-uncompressed.csv")

# Write the 60.txt data to a CSV file with LZ4 compression
splitDF.write.format("csv").mode("overwrite").option("header", "true").option("compression", "lz4").save("s3a://sbalkunde/60-compressed.csv")

# Use the coalesce() function to write the CSV down to a single partition
splitDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("s3a://sbalkunde/60.csv")

# Write the 60.txt data to a Parquet file
splitDF.write.format("parquet").mode("overwrite").save("s3a://sbalkunde/60.parquet")

# Close the Spark session
spark.stop()