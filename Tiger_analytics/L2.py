# Databricks notebook source
''' 
Q1: 
We have the following table
+--------------+------------+-----------------+------+
|candidate_name|candidate_id|year_of_interview|joined|
+--------------+------------+-----------------+------+
|         Alice|           1|             2024|    No|
|           Bob|           2|             2024|   Yes|
|       Charlie|           3|             2024|   Yes|
|         David|           4|             2023|   Yes|
|           Eve|           5|             2022|   Yes|
+--------------+------------+-----------------+------+

# Show the output in following format
+-------------------+--------------------+
|cnt_of_current_year|cnt_of_previous_year|
+-------------------+--------------------+
|                 16|                  14|
+-------------------+--------------------+
'''


# COMMAND ----------

# MAGIC %run ./candidate_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC Q1: Display number of candidates for each year in different columns

# COMMAND ----------

result = spark.sql(
    """
    
    SELECT
        SUM(CASE WHEN year_of_interview = year(current_date()) THEN 1 ELSE 0 END) AS cnt_of_current_year,
        SUM(CASE WHEN year_of_interview = year(current_date())-1 THEN 1 ELSE 0 END) AS cnt_of_previous_year
    FROM candidate_tbl

"""
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2: Find the conversion rate for the each year for the same table

# COMMAND ----------

result1 = spark.sql(
    """
    with cte as (
    SELECT
        SUM(CASE WHEN year_of_interview = year(current_date()) THEN 1 ELSE 0 END) AS cnt_of_current_year,
        SUM(CASE WHEN year_of_interview = year(current_date())-1 THEN 1 ELSE 0 END) AS cnt_of_previous_year,
        SUM(CASE WHEN year_of_interview = year(current_date()) and joined = 'Yes' THEN 1 ELSE 0 END) AS cnt_of_joined_current_year,
        SUM(CASE WHEN year_of_interview = year(current_date())-1 and joined = 'No' THEN 1 ELSE 0 END) AS cnt_of_joined_previous_year
    FROM candidate_tbl
    )
    select 
    cnt_of_joined_current_year/cnt_of_current_year as conversion_rate_current_year,
    cnt_of_joined_previous_year/cnt_of_previous_year as conversion_rate_previous_year
    from cte

"""
)
result1.show()
