# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id as merge_key, updates.*
            FROM updates

            UNION ALL

            SELECT NULL as merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
          ) staged_updates
        ON books_silver.book_id = merge_key 
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
          UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
          INSERT (book_id, title, author, price, current, effective_date, end_date)
          VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

def porcess_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    query = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                 .writeStream
                    .foreachBatch(type2_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
porcess_books()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC    FROM books_silver
# MAGIC    WHERE current IS TRUE
