# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls("/path/to/bronze/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.json("/path/to/bronze/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

files = dbutils.fs.ls("/path/to/bronze/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.parquet("/path/to/bronze/_delta_log/00000000000000000010.checkpoint.parquet"))
