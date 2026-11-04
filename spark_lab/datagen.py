"""Synthetic, deterministic retail data built with spark.range (no files to upload).

Scale up/down with the n_* arguments. Free Edition has quotas - start small
(1-5M orders) and increase once things work.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
SEGMENTS = ["consumer", "smb", "enterprise"]
CATEGORIES = ["electronics", "grocery", "apparel", "home", "toys", "sports"]


def _pick(values, seed_col):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.abs(F.hash(seed_col)) % len(values) + 1).cast("int"))


def customers(spark: SparkSession, n: int = 100_000) -> DataFrame:
    return (spark.range(n).withColumnRenamed("id", "customer_id")
            .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_id")))
            .withColumn("email", F.concat(F.lit("c"), F.col("customer_id"), F.lit("@example.com")))
            .withColumn("region", _pick(REGIONS, F.col("customer_id")))
            .withColumn("segment", _pick(SEGMENTS, F.col("customer_id") * 7))
            .withColumn("signup_date", F.date_sub(F.lit("2026-01-01").cast("date"), (F.col("customer_id") % 1500).cast("int"))))


def products(spark: SparkSession, n: int = 5_000) -> DataFrame:
    return (spark.range(n).withColumnRenamed("id", "product_id")
            .withColumn("product_name", F.concat(F.lit("sku_"), F.col("product_id")))
            .withColumn("category", _pick(CATEGORIES, F.col("product_id")))
            .withColumn("list_price", F.round(5 + F.rand(11) * 495, 2)))


def orders(spark: SparkSession, n: int = 2_000_000, n_customers: int = 100_000,
           n_products: int = 5_000, hot_customer_fraction: float = 0.0,
           start_date: str = "2025-01-01", days: int = 365) -> DataFrame:
    """Order lines. hot_customer_fraction > 0 sends that share of rows to customer_id 0 (data skew)."""
    uniform_cust = (F.rand(1) * n_customers).cast("long")
    cust = (F.when(F.rand(2) < hot_customer_fraction, F.lit(0).cast("long")).otherwise(uniform_cust)
            if hot_customer_fraction > 0 else uniform_cust)
    return (spark.range(n).withColumnRenamed("id", "order_id")
            .withColumn("customer_id", cust)
            .withColumn("product_id", (F.rand(3) * n_products).cast("long"))
            .withColumn("order_date", F.date_add(F.lit(start_date).cast("date"), (F.rand(4) * days).cast("int")))
            .withColumn("quantity", (F.rand(5) * 5 + 1).cast("int"))
            .withColumn("amount", F.round(F.rand(6) * 200 + 1, 2))
            .withColumn("status", _pick(["placed", "shipped", "delivered", "returned"], F.col("order_id"))))


def raw_orders_with_dupes(spark: SparkSession, n: int = 500_000, dup_fraction: float = 0.05) -> DataFrame:
    """Bronze-style feed: some orders re-sent with a later ingest timestamp (CDC / at-least-once delivery)."""
    base = orders(spark, n).withColumn("ingest_ts", F.to_timestamp(F.col("order_date")))
    dupes = (base.sample(fraction=dup_fraction, seed=9)
             .withColumn("status", F.lit("shipped"))
             .withColumn("ingest_ts", F.col("ingest_ts") + F.expr("INTERVAL 2 DAYS")))
    return base.unionByName(dupes)
