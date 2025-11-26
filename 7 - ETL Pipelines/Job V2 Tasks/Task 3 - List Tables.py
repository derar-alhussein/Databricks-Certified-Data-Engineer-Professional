# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

tables_list = spark.sql(f"""
                    SELECT table_name, table_type
                    FROM system.information_schema.tables
                    WHERE table_catalog = '{catalog}'
                    AND table_schema = '{schema}'
                    AND table_type IN ('STREAMING_TABLE', 'MATERIALIZED_VIEW')
                """).collect()

tables_array = [row.asDict() for row in tables_list]

# COMMAND ----------

dbutils.jobs.taskValues.set("tables", tables_array)
