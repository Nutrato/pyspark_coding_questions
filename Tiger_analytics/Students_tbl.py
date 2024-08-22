# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import random

# Initialize Spark session
spark = SparkSession.builder.appName("StudentsTable").getOrCreate()

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("sub", StringType(), True),
    StructField("marks", IntegerType(), True)
])

# Sample data with 50 rows
names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack",
         "Kara", "Leo", "Mia", "Nina", "Oscar", "Paul", "Quinn", "Rita", "Sam", "Tina",
         "Uma", "Vince", "Walt", "Xena", "Yara", "Zane", "Amy", "Brian", "Cathy", "Derek",
         "Ella", "Fred", "Gina", "Holly", "Ian", "Jill", "Kyle", "Lara", "Mike", "Nate",
         "Olga", "Pete", "Quincy", "Rose", "Steve", "Tara", "Umar", "Vera", "Will", "Xander"]

subjects = ["Math", "Science", "History", "Geography", "English"]

data = [Row(name=random.choice(names), sub=random.choice(subjects), marks=random.randint(50, 100)) for _ in range(50)]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary view
df.createOrReplaceTempView("students")

# Show DataFrame
#df.show()
