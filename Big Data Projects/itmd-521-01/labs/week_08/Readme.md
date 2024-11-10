# Week-08 Lab

## Objectives

- Understand the structure of Spark SQL queries and how to convert them to PySpark DataFrame API calls
- Understand how to make tempViews from Tables
- Understand how to make queries against tempViews
- Understand how to use the Spark Catalog
- Examine the use of the Spark DataFrameReader and Writer to manipulate file data

## Assignment Setup

- We will be using the departure delays files located at:
  - `~/LearningSparkV2/databricks-datasets/learning-spark-v2/flights`
  - See Python sample below for how to structure commandline input of datasources
  - Note do not use the notebook file provided, we want to challenge you to be able to build this yourself
  - All Code will be created on your local system, pushed to GitHub, and then pulled to your Vagrant Box
  - You will create one Pyspark file named: `assignment-03.py`
  - You will create on Scala file named: `assignment-03.scala`
    - And the necessary `sbt` build infrastructure
  - You will continue to load data via a commandline argument passing in the path of the file

### DataFrameReader

When reading data you can use the generic `spark.read.format` command, but I recommend to use the domain specific functions.

* DataFrameReader - [parquet](https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrameReader.parquet.html "webpage for pyspark api parquet")
* DataFrameReader - [json](https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrameReader.json.html "webpage for pyspark api json")
* DataFrameReader - [Scala](https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/DataFrameReader.html "webpage for Spark Scala API")

## Assignment Details - Part I

Using the `departuredelays.csv` file, in a single file called `assignment-03.py` convert the remaining two Spark SQL queries from page 87 into Spark DataFrame APIs. Assume you are reading the input dynamically from the commandline -- no hard coding of file paths.

- Type the remaining Spark SQL examples on page 87 into your code
- Run it to show the results, limit 10 records - capture screenshot of just this result
- Then type the corresponding PySpark DataFrame API below to compare answers
- The code must run and show the desired results

### Part I - Screenshot

### Python SQL Output

![*Part I - Q1 sql*](./images/Query%201_sql%20output.png "Part I - Q1 sql")

![*Part I - Q2 sql*](./images/Query%202_sql%20output.png "Part I - Q2 sql")

### python Dataframe api Output

![*Part I - Q1 api*](./images/Query%201_api%20output.png "Part I - Q1 api")

![*Part I - Q2 api*](./images/Query%202_api%20output.png "Part I - Q2 api")

### scala SQL Output

![*scala Part I - Q1 sql*](./images/scala_Query%201_sql%20output.png "scala Part I - Q1 sql")

![*scala Part I - Q2 sql*](./images/scala_Query%202_sql%20output.png "scala Part I - Q2 sql")

### scala Dataframe api Output

![*scala Part I - Q1 api*](./images/scala_query%201_api%20output.png "scala Part I - Q1 api")

![*scala Part I - Q2 api*](./images/scala_query%202_api%20output.png "scala Part I - Q2 api")



## Assignment Details - Part II

- From page 90-92, you will create a Table named `us_delay_flights_tbl` from the `departuredelays.csv`
  - Create a `tempView` of all flights with an origin of Chicago (ORD) and a month/day combo of between 03/01 and 03/15
  - Show the first 5 records of the tempView, taking a screenshot
  - Use the Spark Catalog to list the columns of table `us_delay_flights_tbl`
- **Note:** Add this line to your `spark-submit` command to overcome the database already exists error
  - `--conf spark.sql.catalogImplementation=hive`
  - Example: `spark-submit --conf spark.sql.catalogImplementation=hive assignment-03.py ./departuredelays.csv`

### Part II - Screenshot

### python

### flights with origin ORD and month/day between 03/01 and 03/15

![*Part II - 1st output*](./images/Part%20II%20_%20filtered%20ord%20flights.png "Part II - 1st output")

### Columns of table us_delay_flights

![*Part II - 2nd output*](./images/Part%20II%20_%20columns%20of%20us_delay_flights_tbl.png "Part II - 2nd output")

### scala

### flights with origin ORD and month/day between 03/01 and 03/15

![*scala Part II - 1st output*](./images/scala_Part%20II%20_%20filtered%20ord%20flights.png "scala Part II - 1st output")

### Columns of table us_delay_flights

![*scala Part II - 2nd output*](./images/scala_Part%20II%20_%20columns%20of%20us_delay_flights_tbl.png "scala Part II - 2nd output")

## Assignment Details - Part III

Using the file: `learning-spark-v2 > flights > departuredelays.csv`

Read the file into a DataFrame, and apply the appropriate schema (ie first column should be of a Date Type). Using a DataFrameWriter, write the content out:

* As a JSON 
* JSON as lz4
* As a Parquet file

For all files use `mode` as `overwrite` so that I can run your program and your work will overwrite any previous work. Keep the filename the same, `departuredelays`.

### python

![*Part III - Output*](./images/Part%20III_json_parquet_lz4%20files.png "Part III - Output")

### scala 

![*scala Part III - Output*](./images/scala_Part%20III_json_parquet_lz4%20files.png "scala Part III - Output")

## Assignment Details - Part IV

Using the `departuredelays` parquet file you created part III, read the content into a DataFrame, select all records that have `ORD` (Chicago O'Hare as `Origin`) and write the results to a DataFrameWriter named `orddeparturedelays`

* Use a `.show(10)` function to print out the first 10 lines, and take a screenshot
* Save as type Parquet 

### Part IV - Screenshot

### python

![*Part IV output*](./images/Part%20IV%20output.png "Part IV output")

### scala

![*scala Part IV output*](./images/Scala_Part%20IV%20output.png "scala Part IV output")

### Deliverable

Create a sub-folder named: `labs` under the `itmd-521` folder, named `week-08`. Place all deliverables there.
Submit to Blackboard the URL to the folder in your GitHub repo.  I will clone your code and run it to test the functionality. I don't need the datasets as I will have them already.

Submit to Blackboard the URL to the folder in your GitHub repo.  I will clone your code and run it to test the functionality. I don't need the datasets as I will have them configured in the example-data directory -- path is important.

Due at the **Start of class** Section 05 March 6th 1:50 PM CST
Due at the **Start of class** Section 01,02,03 March 7th 3:15 PM CST


[def]: ./images/Query%201_sql%20output.png "Part I"