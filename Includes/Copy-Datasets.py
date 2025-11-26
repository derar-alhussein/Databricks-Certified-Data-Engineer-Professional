# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# COMMAND ----------

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    msg = str(e)
    if ("com.databricks.sql.io.CloudFileNotFoundException" in msg
        or "java.io.FileNotFoundException" in msg):
      return False
    else:
      raise

# COMMAND ----------

class CourseDataset:
    def __init__(self, uri, data_catalog, db_name, location=None, checkpoint=None):
        self.uri = uri
        self.dataset_path = location
        self.checkpoint_path = checkpoint
        self.catalog_name = data_catalog
        self.db_name = db_name
    
    def download_dataset(self):
        source = self.uri
        target = self.dataset_path

        if self.catalog_name == "hive_metastore":
            try:
                spark.conf.set("fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
                spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
            except:
                pass

        files = dbutils.fs.ls(source)

        for f in files:
            source_path = f"{source}/{f.name}"
            target_path = f"{target}/{f.name}"
            if not path_exists(target_path):
                print(f"Copying {f.name} ...")
                dbutils.fs.cp(source_path, target_path, True)
    
    
    def create_database(self):
        spark.sql(f"USE CATALOG {self.catalog_name}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.db_name}")
        spark.sql(f"USE SCHEMA {self.db_name}")

        print(f"Data catalog: {self.catalog_name}")
        print(f"Schema: {self.db_name}")

        self.__configure_directories()
    
    
    def clean_up(self):
        if self.catalog_name == "hive_metastore":
            print("Removing Checkpoints ...")
            dbutils.fs.rm(self.checkpoint_path, True)
            print("Dropping Database ...")
            spark.sql(f"DROP SCHEMA IF EXISTS {self.db_name} CASCADE")
            print("Removing Dataset ...")
            dbutils.fs.rm(self.dataset_path, True)
        else:
            print("Dropping Database, Dataset, and Checkpoints ...")
            spark.sql(f"DROP SCHEMA IF EXISTS {self.db_name} CASCADE")
        print("Done")


    def __configure_directories(self):
        dataset_volume_name = "dataset"
        checkpoints_volume_name = "checkpoints"

        if self.catalog_name == "hive_metastore":
            self.dataset_path = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore'
            self.checkpoint_path = "dbfs:/mnt/demo_pro/checkpoints"
        else:
            volume_root = f"/Volumes/{self.catalog_name}/{self.db_name}"
            self.dataset_path = f"{volume_root}/{dataset_volume_name}"
            self.checkpoint_path = f"{volume_root}/{checkpoints_volume_name}"
            
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {dataset_volume_name}")
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoints_volume_name}")
    
    def __get_index(self, dir):
        try:
            files = dbutils.fs.ls(dir)
            file = max(f.name for f in files if f.name.endswith('.json'))
            index = int(file.rsplit('.', maxsplit=1)[0])
        except:
            index = 0
        return index+1
    
    
    def __load_json_file(self, current_index, streaming_dir, raw_dir):
        latest_file = f"{str(current_index).zfill(2)}.json"
        source = f"{streaming_dir}/{latest_file}"
        target = f"{raw_dir}/{latest_file}"
        prefix = streaming_dir.split("/")[-1]
        if path_exists(source):
            print(f"Loading {prefix}-{latest_file} file to the bookstore dataset")
            dbutils.fs.cp(source, target)
    
    
    def __load_data(self, max, streaming_dir, raw_dir, all=False):
        index = self.__get_index(raw_dir)
        if index > max:
            print("No more data to load\n")
            return 0

        elif all == True:
            while index <= max:
                self.__load_json_file(index, streaming_dir, raw_dir)
                index += 1
        else:
            self.__load_json_file(index, streaming_dir, raw_dir)
            index += 1

        return 1
    
    def load_new_data(self, num_files = 1):
        streaming_dir = f"{self.dataset_path}/kafka-streaming"
        raw_dir = f"{self.dataset_path}/kafka-raw"
        for i in range(num_files):
            self.__load_data(10, streaming_dir, raw_dir)

    def load_books_updates(self):
        streaming_dir = f"{self.dataset_path}/books-updates-streaming"
        raw_dir = f"{self.dataset_path}/kafka-raw/books-updates"
        self.__load_data(5, streaming_dir, raw_dir)

    def load_pipeline_data(self):
        streaming_dir = f"{self.dataset_path}/kafka-streaming"
        raw_dir = f"{self.dataset_path}/kafka-raw-etl"
        n = self.__load_data(10, streaming_dir, raw_dir)

        books_streaming_dir = f"{self.dataset_path}/books-updates-streaming"
        books_raw_dir = f"{self.dataset_path}/kafka-raw-etl/books-updates"
        m = self.__load_data(5, books_streaming_dir, books_raw_dir)

        return n + m
 
    def process_bronze(self):
        schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

        query = (spark.readStream
                            .format("cloudFiles")
                            .option("cloudFiles.format", "json")
                            .schema(schema)
                            .load(f"{self.dataset_path}/kafka-raw")
                            .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                            .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                      .writeStream
                          .option("checkpointLocation", f"{self.checkpoint_path}/bronze")
                          .option("mergeSchema", True)
                          .partitionBy("topic", "year_month")
                          .trigger(availableNow=True)
                          .table("bronze"))

        query.awaitTermination()
    
    @staticmethod
    def upsert_orders_batch(microBatchDF, batch):
        microBatchDF.createOrReplaceTempView("orders_microbatch")
    
        sql_query = """
          MERGE INTO orders_silver a
          USING orders_microbatch b
          ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
          WHEN NOT MATCHED THEN INSERT *
        """

        microBatchDF.sparkSession.sql(sql_query)
        
    @staticmethod
    def upsert_customers_batch(microBatchDF, batchId):
        window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
        
        (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                     .withColumn("rank", F.rank().over(window))
                     .filter("rank == 1")
                     .drop("rank")
                     .createOrReplaceTempView("ranked_updates"))

        query = """
            MERGE INTO customers_silver c
            USING ranked_updates r
            ON c.customer_id=r.customer_id
                WHEN MATCHED AND c.row_time < r.row_time
                  THEN UPDATE SET *
                WHEN NOT MATCHED
                  THEN INSERT *
        """

        microBatchDF.sparkSession.sql(query)
        
    @staticmethod
    def upsert_books_batch(microBatchDF, batch):
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
    
    def process_orders_silver(self):
        json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
        
        deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))
        
        query = (deduped_df.writeStream
                   .foreachBatch(CourseDataset.upsert_orders_batch)
                   .outputMode("update")
                   .option("checkpointLocation", f"{self.checkpoint_path}/orders_silver")
                   .trigger(availableNow=True)
                   .start())

        query.awaitTermination()

        
    def process_customers_silver(self):
        
        schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"
        
        df_country_lookup = spark.read.json(f"{self.dataset_path}/country_lookup")

        query = (spark.readStream
                          .table("bronze")
                          .filter("topic = 'customers'")
                          .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                          .select("v.*")
                          .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
                       .writeStream
                          .foreachBatch(CourseDataset.upsert_customers_batch)
                          .outputMode("update")
                          .option("checkpointLocation", f"{self.checkpoint_path}/customers_silver")
                          .trigger(availableNow=True)
                          .start()
                )

        query.awaitTermination()
    
    def process_books_silver(self):
        schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"

        query = (spark.readStream
                        .table("bronze")
                        .filter("topic = 'books'")
                        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                        .select("v.*")
                     .writeStream
                        .foreachBatch(CourseDataset.upsert_books_batch)
                        .option("checkpointLocation", f"{self.checkpoint_path}/books_silver")
                        .trigger(availableNow=True)
                        .start()
                )

        query.awaitTermination()
        
    def process_current_books(self):
        spark.sql("""
            CREATE OR REPLACE TABLE current_books
            AS SELECT book_id, title, author, price
               FROM books_silver
               WHERE current IS TRUE
        """)

# COMMAND ----------

data_source_uri = "s3://dalhussein-courses/DE-Pro/datasets/bookstore/v1/"
db_name = "bookstore_eng_pro"

data_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
bookstore = CourseDataset(data_source_uri, data_catalog, db_name)

bookstore.create_database()
bookstore.download_dataset()
