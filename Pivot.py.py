# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[
('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math', 68),
('Shivu','eng', 59),
('Anu','math', 65),
('Anu','eng',80)
]
schema=["Name", "Sub", "Marks"]
df=spark.createDataFrame(data,schema)
df.show()


# COMMAND ----------

df1 = df.groupBy(col("Name")).agg(collect_list(col("Marks")).alias("New_Mrks"))
df1 = df1.select(col("Name"), col("New_Mrks")[0].alias("MATH"), col("New_Mrks")[1].alias("ENG"))
df1.show()
