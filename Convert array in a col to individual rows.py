import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window as W

# Initialize Spark session
spark = SparkSession.builder.appName("table").getOrCreate()



# COMMAND ----------

data = [(1, "abc", "Maths, physics, Chemsitry"), (2, "def", "Maths, physics")]

schema = ["id", "sname", "sub"]

df = spark.createDataFrame(data, schema)

df.show()

# COMMAND ----------

df1 = df.withColumn("sub_explode", split(col("sub"),","))\
    .withColumn("subs", explode(col("sub_explode"))).drop("sub","sub_explode")
df1.show()
