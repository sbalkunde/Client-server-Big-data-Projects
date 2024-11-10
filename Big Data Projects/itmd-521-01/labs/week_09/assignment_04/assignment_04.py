from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql.functions import current_date, lit, when

# Create SparkSession
spark = SparkSession.builder.appName("Assignment_04_py").getOrCreate()

########### Part I ###########

# Read employees table into a DataFrame
employees_df = (spark.read.format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/employees")
                .option("driver","com.mysql.jdbc.Driver")
                .option("dbtable", "employees")
                .option("user", "sbalkunde")
                .option("password", "bigdata")
                .load())

# Display count of number of records
print("Number of records in employees table:", employees_df.count())

# Display schema of Employees Table
print("Schema of Employees Table:")
employees_df.printSchema()

#show top 5 rows of Employees Table
employees_df.show(5)

# Create a DataFrame of the top 10,000 employee salaries (sort in DESC order) from the salaries table
salaries_df = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver","com.mysql.jdbc.Driver")
    .option("dbtable", "salaries")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .load())

top_salaries_df = salaries_df.orderBy(salaries_df["salary"].desc()).limit(10000)

# Saving data to a JDBC source using save by writing DataFrame back to database to a new table called: aces
(top_salaries_df.write 
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "aces")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .mode("overwrite")
    .save())

# Write DataFrame out to the local system as a CSV with snappy compression
top_salaries_df.write.csv("/home/vagrant/sbalkunde/itmd-521-01/labs/week_09/assignment_04/aces.csv", compression="snappy", mode="overwrite")

####### Part II ########

# Create a DataFrame by directly querying the titles table for Senior Engineer
senior_engineers_df = (spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'")
    .load())

# Add a temp column to indicate if the senior engineer is still with the company or has left
senior_engineers_df = senior_engineers_df.withColumn("employment_status", when(col("to_date") == "9999-01-01", "current").otherwise("left"))

# Issue a count of how many senior engineers have left and how many are still with the company
senior_engineers_count = senior_engineers_df.groupBy("employment_status").count()
senior_engineers_count.show()

#1st phase -

# Create a PySpark DataFrame of just the Senior Engineers information that have left the company
senior_engineers_left_df = senior_engineers_df.filter(senior_engineers_df["employment_status"] == "left")

# Create a PySpark SQL table of just the Senior Engineers information that have left the company
(senior_engineers_left_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "senior_engineers_left_table")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .mode("overwrite")
    .save())

# Create a PySpark SQL tempView of just the Senior Engineers information that have left the company
senior_engineers_left_df.createOrReplaceTempView("senior_engineers_left_tempview")

#2nd phase - 
#Write the SQL table back to the database
(senior_engineers_left_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "left_table")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .mode("overwrite")
    .save())

# Write the SQL tempView back to the database
(senior_engineers_left_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .option("dbtable", "left_tempview")
    .mode("overwrite")
    .save())

# Write the DataFrame back to the database
(senior_engineers_left_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "left_df")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .mode("overwrite")
    .save())

# Repeating the previous command for DataFrame with mode type set to errorifexists

(senior_engineers_left_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "left_df")
    .option("user", "sbalkunde")
    .option("password", "bigdata")
    .mode("errorifexists")
    .save())


# Close SparkSession
spark.stop()