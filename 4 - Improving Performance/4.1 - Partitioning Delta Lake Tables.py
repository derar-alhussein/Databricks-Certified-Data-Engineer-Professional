# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze

# COMMAND ----------

files = dbutils.fs.ls("/path/to/bronze")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("/path/to/bronze/topic=customers")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("/path/to/bronze/topic=customers/year_month=2021-12/")
display(files)
