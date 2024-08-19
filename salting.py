# Databricks notebook source
#importing functions
from pyspark.sql.functions import *

# COMMAND ----------

#setting partitions and adaptive query execution false
spark.conf.set("spark.sql.shuffle.partitions",3)
spark.conf.set("spark.sql.adaptive.enabled",'false')

# COMMAND ----------

print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.adaptive.enabled"))

# COMMAND ----------

#creating table1 and table2 with skewed data
id_values = [1] * 10000000 + [2] * 5 + [3]*5
table1 = spark.createDataFrame(id_values,"int").toDF("id")

# COMMAND ----------

#creating table1 and table2 with skewed data
id_values = [1] * 100 + [2] * 5 + [3]*5
table2 = spark.createDataFrame(id_values,"int").toDF("id")

# COMMAND ----------

# creating salted key for table1
df_with_random = table1.withColumn("random_num", (rand() * 10 + 1).cast("int"))

table1 = df_with_random.withColumn(
    "salted_key", concat(expr("id"), lit("-"), expr("random_num"))
)

# COMMAND ----------

# creating salted key for table2

table2_replicated = table2.withColumn("sequence", array([lit(i) for i in range(1, 11)]))

table2 = (
    table2_replicated.withColumn("exploded_col", explode(col("sequence")))
    .withColumn("salted_key", concat(expr("id"), lit("-"), expr("exploded_col")))
    .drop("exploded_col")
)

# COMMAND ----------

# joining on id (withoit salted key)
joined_df = table1.join(table2, on = ["id"], how = "inner")
print(joined_df.count())

# COMMAND ----------

# joining on id (with salted key)

joined_df = table1.join(table2, on = ["salted_key"], how = "inner")
print(joined_df.count())
