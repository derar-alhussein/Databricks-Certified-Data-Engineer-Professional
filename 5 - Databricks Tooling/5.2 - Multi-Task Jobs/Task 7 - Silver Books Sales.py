# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def process_books_sales():
    
    orders_df = (spark.readStream.table("orders_silver")
                        .withColumn("book", F.explode("books"))
                )

    books_df = spark.read.table("current_books")

    query = (orders_df
                  .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
                  .writeStream
                     .outputMode("append")
                     .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_sales")
                     .trigger(availableNow=True)
                     .table("books_sales")
    )

    query.awaitTermination()
    
process_books_sales()
