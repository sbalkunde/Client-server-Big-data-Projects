from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, year, month, avg, col

# Importing necessary modules for handling environment variables and system operations
# Removing hard coded password - using os module to import them
import os
import sys

# Creating a Spark configuration object to manage Spark configuration parameters
conf = SparkConf()

#Setting JAR Packages-This package contains classes necessary for Spark enabling to interact with AWS services like S3
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')

#Setting AWS Credentials via Spark configuration to interact with S3 buckets
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))

# Configurations specific to S3 interactions
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')

# Setting internal IP for a proxy that routes requests to S3, enhancing network operations
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

# Network settings to prevent timeouts during large data transfers or long operations
conf.set("spark.network.timeout", "800s")
conf.set("spark.executor.heartbeatInterval", "120s")

# Creating Spark Session
spark = SparkSession.builder.appName("part3:sbalkunde 60.txt").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

# Reading a previously transformed Parquet file into a DataFrame
transformed_df = spark.read.parquet("s3a://sbalkunde/60.parquet")

# Calculate the average temperature per month and year
result_df = transformed_df.groupBy(year('ObservationDate').alias('Year'), month('ObservationDate').alias('Month')).avg('AirTemperature').withColumnRenamed('avg(AirTemperature)', 'AvgTemperature')

# Write the results to a Parquet file and saving in the S3 bucket with overwrite mode
result_df.write.mode("overwrite").parquet('s3a://sbalkunde/part-three.parquet')

# Retrieve only the first year of the decade records of result_df and saving in S3 bucket
records_df = result_df.orderBy('Year', 'Month').limit(12) 
records_df.write.format("csv").mode("overwrite").option("header", "true").save("s3a://sbalkunde/part-three.csv")

# Stop the Spark session
spark.stop()
