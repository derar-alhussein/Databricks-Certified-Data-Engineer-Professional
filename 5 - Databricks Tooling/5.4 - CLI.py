# Databricks notebook source
# MAGIC %fs ls 'dbfs:/mnt/uploads'

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

db_password = dbutils.secrets.get("bookstore-dev", "db_password")

# COMMAND ----------

print(db_password)

# COMMAND ----------


