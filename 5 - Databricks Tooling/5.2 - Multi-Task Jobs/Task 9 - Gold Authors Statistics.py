# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

query = (spark.readStream
                 .table("books_sales")
                 .withWatermark("order_timestamp", "10 minutes")
                 .groupBy(
                     F.window("order_timestamp", "5 minutes").alias("time"),
                     "author")
                 .agg(
                     F.count("order_id").alias("orders_count"),
                     F.avg("quantity").alias ("avg_quantity"))
              .writeStream
                 .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                 .trigger(availableNow=True)
                 .table("authors_stats")
            )

query.awaitTermination()
