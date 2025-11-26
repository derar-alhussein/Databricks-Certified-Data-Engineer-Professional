# Databricks notebook source
# MAGIC %run ../../../Includes/Copy-Datasets

# COMMAND ----------

print(bookstore.dataset_path)

# COMMAND ----------

bookstore.load_pipeline_data()
