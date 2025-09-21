# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze

# COMMAND ----------

df = spark.table(
    "bookstore_eng_pro.bronze"
)
display(df)

# COMMAND ----------

df = spark.table(
    "bookstore_eng_pro.bronze"
).where(
    col("topic") == "customers"
)
display(df)

# COMMAND ----------

df = spark.table(
    "bookstore_eng_pro.bronze"
).where(
    (col("topic") == "customers") &
    (col("year_month") == "2021-12")
)
display(df)

# COMMAND ----------


