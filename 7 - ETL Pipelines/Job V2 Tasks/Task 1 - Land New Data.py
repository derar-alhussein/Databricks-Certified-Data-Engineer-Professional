# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

num_files = bookstore.load_pipeline_data()

# COMMAND ----------

dbutils.jobs.taskValues.set("num_new_files", num_files)
