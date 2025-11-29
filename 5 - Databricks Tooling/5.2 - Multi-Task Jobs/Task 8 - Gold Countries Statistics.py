# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )
