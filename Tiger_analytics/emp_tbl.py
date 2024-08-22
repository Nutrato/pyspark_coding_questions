# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import random

# Initialize Spark session
spark = SparkSession.builder.appName("EmpTable").getOrCreate()

# Define schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("managerId", IntegerType(), True),
    StructField("dept_no", IntegerType(), True),
    StructField("location", StringType(), True)
])

# Sample data with dept_no and location
data = [
    Row(1, "Alice", "HR", 50000, random.randint(1, 10), 101, "New York"),
    Row(2, "Bob", "Engineering", 70000, random.randint(1, 10), 102, "San Francisco"),
    Row(3, "Charlie", "Finance", 60000, random.randint(1, 10), 103, "Chicago"),
    Row(4, "David", "Engineering", 75000, random.randint(1, 10), 102, "San Francisco"),
    Row(5, "Eve", "HR", 52000, random.randint(1, 10), 101, "New York"),
    Row(6, "Frank", "Finance", 58000, random.randint(1, 10), 103, "Chicago"),
    Row(7, "Grace", "Engineering", 72000, random.randint(1, 10), 102, "San Francisco"),
    Row(8, "Hank", "HR", 51000, random.randint(1, 10), 101, "New York"),
    Row(9, "Ivy", "Finance", 61000, random.randint(1, 10), 103, "Chicago"),
    Row(10, "Jack", "Engineering", 73000, random.randint(1, 10), 102, "San Francisco"),
    Row(11, "Kara", "HR", 53000, random.randint(1, 10), 101, "New York"),
    Row(12, "Leo", "Finance", 59000, random.randint(1, 10), 103, "Chicago"),
    Row(13, "Mia", "Engineering", 74000, random.randint(1, 10), 102, "San Francisco"),
    Row(14, "Nina", "HR", 54000, random.randint(1, 10), 101, "New York"),
    Row(15, "Oscar", "Finance", 62000, random.randint(1, 10), 103, "Chicago"),
    Row(16, "Paul", "Engineering", 76000, random.randint(1, 10), 102, "San Francisco"),
    Row(17, "Quinn", "HR", 55000, random.randint(1, 10), 101, "New York"),
    Row(18, "Rita", "Finance", 63000, random.randint(1, 10), 103, "Chicago"),
    Row(19, "Sam", "Engineering", 77000, random.randint(1, 10), 102, "San Francisco"),
    Row(20, "Tina", "HR", 56000, random.randint(1, 10), 101, "New York"),
    Row(21, "Uma", "Finance", 64000, random.randint(1, 10), 103, "Chicago"),
    Row(22, "Vince", "Engineering", 78000, random.randint(1, 10), 102, "San Francisco"),
    Row(23, "Walt", "HR", 57000, random.randint(1, 10), 101, "New York"),
    Row(24, "Xena", "Finance", 65000, random.randint(1, 10), 103, "Chicago"),
    Row(25, "Yara", "Engineering", 79000, random.randint(1, 10), 102, "San Francisco"),
    Row(26, "Zane", "HR", 58000, random.randint(1, 10), 101, "New York"),
    Row(27, "Amy", "Finance", 66000, random.randint(1, 10), 103, "Chicago"),
    Row(28, "Brian", "Engineering", 80000, random.randint(1, 10), 102, "San Francisco"),
    Row(29, "Cathy", "HR", 59000, random.randint(1, 10), 101, "New York"),
    Row(30, "Derek", "Finance", 67000, random.randint(1, 10), 103, "Chicago"),
    Row(31, "Ella", "Engineering", 81000, random.randint(1, 10), 102, "San Francisco"),
    Row(32, "Fred", "HR", 60000, random.randint(1, 10), 101, "New York"),
    Row(33, "Gina", "Finance", 68000, random.randint(1, 10), 103, "Chicago"),
    Row(34, "Holly", "Engineering", 82000, random.randint(1, 10), 102, "San Francisco"),
    Row(35, "Ian", "HR", 61000, random.randint(1, 10), 101, "New York"),
    Row(36, "Jill", "Finance", 69000, random.randint(1, 10), 103, "Chicago"),
    Row(37, "Kyle", "Engineering", 83000, random.randint(1, 10), 102, "San Francisco"),
    Row(38, "Lara", "HR", 62000, random.randint(1, 10), 101, "New York"),
    Row(39, "Mike", "Finance", 70000, random.randint(1, 10), 103, "Chicago"),
    Row(40, "Nate", "Engineering", 84000, random.randint(1, 10), 102, "San Francisco"),
    Row(41, "Olga", "HR", 63000, random.randint(1, 10), 101, "New York"),
    Row(42, "Pete", "Finance", 71000, random.randint(1, 10), 103, "Chicago"),
    Row(43, "Quincy", "Engineering", 85000, random.randint(1, 10), 102, "San Francisco"),
    Row(44, "Rose", "HR", 64000, random.randint(1, 10), 101, "New York"),
    Row(45, "Steve", "Finance", 72000, random.randint(1, 10), 103, "Chicago"),
    Row(46, "Tara", "Engineering", 86000, random.randint(1, 10), 102, "San Francisco"),
    Row(47, "Umar", "HR", 65000, random.randint(1, 10), 101, "New York"),
    Row(48, "Vera", "Finance", 73000, random.randint(1, 10), 103, "Chicago"),
    Row(49, "Will", "Engineering", 87000, random.randint(1, 10), 102, "San Francisco"),
    Row(50, "Xander", "HR", 66000, random.randint(1, 10), 101, "New York")
]

# Create DataFrame
df_emp = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary view
df_emp.createOrReplaceTempView("emp")

# Show DataFrame
#df_emp.show()

