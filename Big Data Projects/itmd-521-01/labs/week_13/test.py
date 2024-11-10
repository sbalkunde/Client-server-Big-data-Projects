from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_date
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("Temperature Analysis").getOrCreate()

# Read the data
df = spark.read.text("1950-sample-tail.txt")

# Cleaning & structuring raw data by splitting values into multiple columns based on predefined positions
splitDF = df.withColumn('WeatherStation', df['value'].substr(5, 6)) \
    .withColumn('WBAN', df['value'].substr(11, 5)) \
    .withColumn('ObservationDate', to_date(df['value'].substr(16, 8), 'yyyyMMdd')) \
    .withColumn('ObservationHour', df['value'].substr(24, 4).cast(IntegerType())) \
    .withColumn('Latitude', df['value'].substr(29, 6).cast('float') / 1000) \
    .withColumn('Longitude', df['value'].substr(35, 7).cast('float') / 1000) \
    .withColumn('Elevation', df['value'].substr(47, 5).cast(IntegerType())) \
    .withColumn('WindDirection', df['value'].substr(61, 3).cast(IntegerType())) \
    .withColumn('WDQualityCode', df['value'].substr(64, 1).cast(IntegerType())) \
    .withColumn('SkyCeilingHeight', df['value'].substr(71, 5).cast(IntegerType())) \
    .withColumn('SCQualityCode', df['value'].substr(76, 1).cast(IntegerType())) \
    .withColumn('VisibilityDistance', df['value'].substr(79, 6).cast(IntegerType())) \
    .withColumn('VDQualityCode', df['value'].substr(86, 1).cast(IntegerType())) \
    .withColumn('AirTemperature', df['value'].substr(88, 5).cast('float') / 10) \
    .withColumn('ATQualityCode', df['value'].substr(93, 1).cast(IntegerType())) \
    .withColumn('DewPoint', df['value'].substr(94, 5).cast('float')) \
    .withColumn('DPQualityCode', df['value'].substr(99, 1).cast(IntegerType())) \
    .withColumn('AtmosphericPressure', df['value'].substr(100, 5).cast('float') / 10) \
    .withColumn('APQualityCode', df['value'].substr(105, 1).cast(IntegerType())).drop('value')


# Print schema to verify column types
splitDF.printSchema()

# Show first 5 rows to confirm data transformation
splitDF.show(5)

# Group by year and month, and calculate the average temperature
result_df = splitDF.groupBy(year('ObservationDate').alias('Year'), month('ObservationDate').alias('Month')) \
    .avg('AirTemperature') \
    .withColumnRenamed('avg(AirTemperature)', 'AvgTemperature') \
    .orderBy('Year', 'Month')  # Ordering to ensure the results are chronological

# Display the aggregated DataFrame
result_df.show()

# Write the results to a Parquet file, overwriting any existing file with the same name
result_df.write.mode("overwrite").parquet('part-three.parquet')

# Take only the first 12 records from the year 1950
first_year_df = result_df.filter(col('Year') == 1950).orderBy('Month').limit(12)

# Write the first 12 records to a CSV file, overwriting any existing file with the same name
first_year_df.write.format("csv").mode("overwrite").option("header", "true").save("part-three.csv")

# Stop the Spark session
spark.stop()