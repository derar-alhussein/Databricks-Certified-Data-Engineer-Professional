from pyspark import pipelines as dp
from pyspark.sql import functions as F


def process_orders():
    json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

    return (spark.readStream.table("bronze")
                            .filter("topic = 'orders'")
                            .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                            .select("v.*"))
    

@dp.table
@dp.expect_or_drop("valid_quantity", "quantity > 0")
def orders_silver():
    orders_df = process_orders()
    return orders_df


@dp.table
@dp.expect_or_drop("valid_quantity", "quantity <= 0")
def orders_quarantine():
    orders_df = process_orders()
    return orders_df


rules = {
    "recent_updates": "updated >= '2020-01-01'",
    "valid_price": "price BETWEEN 0 AND 100",
    "valid_id": "book_id IS NOT NULL"
}

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dp.temporary_view
@dp.expect_all(rules)
def books_raw():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"

    return (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                    .withColumn("is_quarantined", F.expr(quarantine_rules))
            )

