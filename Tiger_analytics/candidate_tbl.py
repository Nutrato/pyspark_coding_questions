# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import random

# Initialize Spark session
spark = SparkSession.builder.appName("CandidateTable").getOrCreate()

# Define schema
schema = StructType([
    StructField("candidate_name", StringType(), True),
    StructField("candidate_id", IntegerType(), True),
    StructField("year_of_interview", IntegerType(), True),
    StructField("joined", StringType(), True)
])

# Sample data with random years from 2019 to 2024 and random "Yes" or "No" for "joined"
data = [
    Row("Alice", 1, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Bob", 2, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Charlie", 3, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("David", 4, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Eve", 5, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Frank", 6, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Grace", 7, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Hank", 8, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Ivy", 9, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Jack", 10, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Kara", 11, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Leo", 12, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Mia", 13, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Nina", 14, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Oscar", 15, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Paul", 16, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Quinn", 17, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Rita", 18, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Sam", 19, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Tina", 20, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Uma", 21, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Vince", 22, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Walt", 23, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Xena", 24, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Yara", 25, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Zane", 26, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Amy", 27, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Brian", 28, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Cathy", 29, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Derek", 30, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Ella", 31, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Fred", 32, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Gina", 33, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Holly", 34, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Ian", 35, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Jill", 36, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Kyle", 37, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Lara", 38, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Mike", 39, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Nate", 40, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Olga", 41, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Pete", 42, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Quincy", 43, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Rose", 44, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Steve", 45, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Tara", 46, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Umar", 47, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Vera", 48, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Will", 49, random.randint(2023, 2024), random.choice(["Yes", "No"])),
    Row("Xander", 50, random.randint(2023, 2024), random.choice(["Yes", "No"]))
]

# Create DataFrame
df_candidate = spark.createDataFrame(data, schema)

# Show DataFrame
#df.show()
df_candidate.createOrReplaceTempView("candidate_tbl")
