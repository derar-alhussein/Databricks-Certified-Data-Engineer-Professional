# Databricks notebook source
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_time", "")

job_id = dbutils.widgets.get("job_id")
job_time = dbutils.widgets.get("job_time")

# COMMAND ----------

print(f"Warning: No data to process for job {job_id} at {job_time}")
