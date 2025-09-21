# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key , value 
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# Three differnet Options being illustrated.....

# COMMAND ----------

# OPTION 1

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

orders_silver = (
  spark.readStream
  .table("bronze_tmp")
  .where("topic = 'orders'")
  .selectExpr(
    "from_json(cast(value AS STRING), 'order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>') as v"
  )
  .select("v.*")
)

orders_silver_query = (
  orders_silver.writeStream
  .format("delta")
  .option(
    "checkpointLocation",
    "/Volumes/udemy/default/vrams/demo_pro/checkpoints/orders_silver"
  )
  .trigger(
    availableNow=True
  )
  .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_silver

# COMMAND ----------

dbutils.fs.rm(f"{checkpoint_path}/orders_silver", True)

# COMMAND ----------

# OPTION 2
 
df = spark.sql("""
SELECT v.*
FROM (
  SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
  FROM bronze_tmp
  WHERE topic = 'orders'
) v
""")

df.writeStream \
  .option("checkpointLocation", "/Volumes/udemy/default/vrams/demo_pro/checkpoints/orders_silver") \
  .trigger(availableNow=True) \
  .table("orders_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "/Volumes/udemy/default/vrams/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_silver

# COMMAND ----------

dbutils.fs.rm(f"{checkpoint_path}/orders_silver", True)

# COMMAND ----------

# OPTION 3

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "/Volumes/udemy/default/vrams/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
