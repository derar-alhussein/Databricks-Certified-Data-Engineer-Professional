# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()
