# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
table_type = dbutils.widgets.get("table_type")

# COMMAND ----------

print(f"Analysis of {table_name} of type {table_type}")

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM `{catalog}`.`{schema}`.`{table_name}`").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}`").display()
