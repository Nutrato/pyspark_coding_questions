# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data = [(1, "abc", "Maths, physics, Chemsitry"), (2, "def", "Maths, physics")]

schema = ["id", "sname", "sub"]

df = spark.createDataFrame(data, schema)

df.show()

# COMMAND ----------

df1 = df.withColumn("sub_explode", split(col("sub"),","))\
    .withColumn("subs", explode(col("sub_explode"))).drop("sub","sub_explode")
df1.show()
