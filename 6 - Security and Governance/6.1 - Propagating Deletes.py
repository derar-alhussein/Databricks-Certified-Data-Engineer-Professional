# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/deletes.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

(spark.readStream
        .table("bronze")
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*", F.col('v.row_time').alias("request_timestamp"))
        .filter("row_status = 'delete'")
        .select("customer_id", "request_timestamp",
                F.date_add("request_timestamp", 30).alias("deadline"), 
                F.lit("requested").alias("status"))
    .writeStream
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/delete_requests")
        .trigger(availableNow=True)
        .table("delete_requests")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers_silver
# MAGIC WHERE customer_id IN (SELECT customer_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 2)
                 .table("customers_silver"))

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM customers_orders
        WHERE customer_id IN (SELECT customer_id FROM deletes)
    """)
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests r
        USING deletes d
        ON d.customer_id = r.customer_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

(deleteDF.writeStream
         .foreachBatch(process_deletes)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/deletes")
         .trigger(availableNow=True)
         .start())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders@v6
# MAGIC EXCEPT
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("customers_silver")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------


