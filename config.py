import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window as W

# Initialize Spark session
spark = SparkSession.builder.appName("table").getOrCreate()
