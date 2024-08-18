# Databricks notebook source
# MAGIC %run ./config.py

# COMMAND ----------

emp_data = [(1,"manish","11-07-2023","10:20"),
        (1,"manish","11-07-2023","11:20"),
        (2,"rajesh","11-07-2023","11:20"),
        (1,"manish","11-07-2023","11:50"),
        (2,"rajesh","11-07-2023","13:20"),
        (1,"manish","11-07-2023","19:20"),
        (2,"rajesh","11-07-2023","17:20"),
        (1,"manish","12-07-2023","10:32"),
        (1,"manish","12-07-2023","12:20"),
        (3,"vikash","12-07-2023","09:12"),
        (1,"manish","12-07-2023","16:23"),
        (3,"vikash","12-07-2023","18:08")]

emp_schema = ["id", "name", "date", "time"]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

# COMMAND ----------

#from pyspark.sql.functions import *
#from pyspark.sql.window import Window as W

# COMMAND ----------

df = emp_df.withColumn("time_stamp", 
                       from_unixtime(unix_timestamp(expr("CONCAT(date,' ', time)"),"dd-MM-yyyy HH:mm")))
df.show()

# COMMAND ----------

emp_df = emp_df.withColumn("time", concat(col("date"), lit(" "), col("time")))\
           .withColumn("time_stamp", to_timestamp(col("time"), "dd-MM-yyyy HH:mm"))       
df.show()

# COMMAND ----------

window_spec = W.partitionBy("id", "date").orderBy("time_stamp").rangeBetween(W.unboundedPreceding,W.unboundedFollowing)

# COMMAND ----------

df = emp_df.withColumn("login", first("time_stamp").over(window_spec))\
           .withColumn("logout", last("time_stamp").over(window_spec))\
            .withColumn("IN_TIME", lit(col("logout").cast("long")-col("login").cast("long")))\
            .show()
#            .withColumn(when(col("IN_TIME")>=8, lit("Complient").otherwise("Non-Complient")))
#df.printSchema()

# COMMAND ----------

df = emp_df.withColumn("login", first("time_stamp").over(window_spec))\
           .withColumn("logout", last("time_stamp").over(window_spec))\
            .withColumn("IN_TIME", lit(col("logout").cast("long")-col("login").cast("long")))\
            .filter(col("IN_TIME") < 8*60*60)\
            .select("id", "name", "date")\
            .distinct().show()
#            .withColumn(when(col("IN_TIME")>=8, lit("Complient").otherwise("Non-Complient")))
#df.printSchema()
