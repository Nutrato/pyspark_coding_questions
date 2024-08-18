# Databricks notebook source
# MAGIC %md
# MAGIC Find differnece between first month and last month sales

# COMMAND ----------

# MAGIC %run ./config.py

# COMMAND ----------

product_data = [
(2,"samsung","01-01-1995",11000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-01-2006",15000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(3,"oneplus","01-01-2010",23000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

# COMMAND ----------

products_df = spark.createDataFrame(data = product_data, schema = product_schema)

# COMMAND ----------

products_df.show()

# COMMAND ----------

#from pyspark.sql.functions import *
#from pyspark.sql.window import Window as W

# COMMAND ----------

window_spec = W.partitionBy("product_id").orderBy("sales_date").rangeBetween(W.unboundedPreceding,W.unboundedFollowing)

# COMMAND ----------

unique_product_df = (products_df.withColumn("First_sales", first("sales").over(window_spec))
                     .withColumn("Last_sales",last("sales").over(window_spec))
                     .withColumn("Sales_Difference", lit(col("Last_sales")-col("First_sales")))
                     .drop("sales")
                     .drop("sales_date")
                     .dropDuplicates("product_id")
                     )

# COMMAND ----------

unique_product_df.show()

# COMMAND ----------


