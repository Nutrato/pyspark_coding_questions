# Databricks notebook source
# MAGIC %run ./config.py

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)


# COMMAND ----------

#product_df.collect()[1]["product_name"]

# COMMAND ----------

product_df.orderBy("product_id","sales_date").show()

# COMMAND ----------

window_spec = W.partitionBy("product_id").orderBy("sales_date").rowsBetween(-2,0)
window_spec1 = W.partitionBy("product_id").orderBy("sales_date")

# COMMAND ----------

product_df = product_df.withColumn("running_sum", sum("sales").over(window_spec))\
                       .withColumn("3months_avg", round(col("running_sum") / 3, 2))\
                       .withColumn("row_number", row_number().over(window_spec1))\
                        .filter(col("row_number")>2)
product_df.show()

# COMMAND ----------


