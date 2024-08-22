# Databricks notebook source
# MAGIC %run ./Students_tbl.py

# COMMAND ----------

df.createOrReplaceTempView("Students")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Q1: find top 5 scoring students
# MAGIC --select s.name from 
# MAGIC select name, sub,
# MAGIC rank() over(partition by sub order by marks desc) as rnk
# MAGIC from students
# MAGIC --) s
# MAGIC --where s.rnk<=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, sum(marks) from students
# MAGIC group by name having sum(marks)>400

# COMMAND ----------

# MAGIC %run ./emp_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: find manager name
# MAGIC SELECT e1.name AS employee_name, e1.emp_id as empID, e2.name AS manager_name, e2.emp_id as mgrID
# MAGIC FROM emp e1
# MAGIC LEFT JOIN emp e2 ON e1.managerId = e2.emp_id
# MAGIC
