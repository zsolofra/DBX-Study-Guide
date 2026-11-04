# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Warehousing on the Lakehouse (your technical spike)
# MAGIC Build a real star schema end-to-end with the medallion architecture:
# MAGIC
# MAGIC ```
# MAGIC  BRONZE (raw, append-only, dupes)  ->  SILVER (deduped, typed, validated)  ->  GOLD (star schema)
# MAGIC  dw_bronze_orders                      dw_silver_orders                       dim_date
# MAGIC  dw_bronze_customers                                                         dim_customer  (SCD2)
# MAGIC                                                                              dim_product   (SCD1)
# MAGIC                                                                              fact_sales    (grain: one order line)
# MAGIC ```
# MAGIC At each step, practice explaining **the design decision** and **the Databricks feature** behind it.
# MAGIC Companion reading: `docs/data_warehousing_spike.md`.

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)

from pyspark.sql import functions as F
from spark_lab import datagen, techniques as T
from warehouse.delta_scd import scd2_merge_sql

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

def try_sql(stmt):
    """Some features depend on workspace tier/compute - show the error instead of failing the notebook."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        print(f"[skipped] {type(e).__name__}: {str(e).splitlines()[0][:200]}")

# COMMAND ----------

# MAGIC %md ## 1. Bronze: land raw data as-is (append-only, keep ingest metadata)

# COMMAND ----------

datagen.raw_orders_with_dupes(spark, 1_000_000).write.mode("overwrite").saveAsTable("dw_bronze_orders")
datagen.customers(spark, 100_000).write.mode("overwrite").saveAsTable("dw_bronze_customers")
datagen.products(spark, 5_000).write.mode("overwrite").saveAsTable("dw_bronze_products")
display(spark.sql("SELECT count(*) AS rows, count(DISTINCT order_id) AS distinct_orders FROM dw_bronze_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: dedupe, validate, conform
# MAGIC Duplicates come from at-least-once delivery / CDC replays. Keep the **latest** version per business key.

# COMMAND ----------

bronze = spark.table("dw_bronze_orders")
silver = T.latest_per_key_window(bronze, "order_id", "ingest_ts")

checks = silver.agg(
    F.count("*").alias("rows"),
    F.sum(F.col("amount").isNull().cast("int")).alias("null_amount"),
    F.sum((F.col("amount") <= 0).cast("int")).alias("non_positive_amount"),
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer"),
)
display(checks)
silver.write.mode("overwrite").saveAsTable("dw_silver_orders")

# Enforced constraint: Delta rejects any future write that violates it.
try_sql("ALTER TABLE dw_silver_orders ADD CONSTRAINT positive_amount CHECK (amount > 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "In production I'd make this a Lakeflow Declarative Pipeline with expectations
# MAGIC (`EXPECT ... ON VIOLATION DROP ROW / FAIL UPDATE`) so data quality is declarative and tracked in the event log,
# MAGIC and use Auto Loader for incremental ingestion with schema evolution into bronze."

# COMMAND ----------

# MAGIC %md ## 3. dim_date (generated, not loaded)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_date AS
# MAGIC SELECT
# MAGIC   CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_sk,   -- smart key: readable, sortable, cluster-friendly
# MAGIC   d                                        AS full_date,
# MAGIC   year(d) AS year, quarter(d) AS quarter, month(d) AS month,
# MAGIC   date_format(d, 'MMMM') AS month_name,
# MAGIC   weekofyear(d) AS week_of_year, dayofweek(d) AS day_of_week,
# MAGIC   dayofweek(d) IN (1, 7) AS is_weekend
# MAGIC FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS d);
# MAGIC
# MAGIC ALTER TABLE dim_date ALTER COLUMN date_sk SET NOT NULL;

# COMMAND ----------

try_sql("ALTER TABLE dim_date ADD CONSTRAINT dim_date_pk PRIMARY KEY (date_sk) RELY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_customer as SCD Type 2
# MAGIC - Surrogate key from an **IDENTITY** column (stable join key, decouples from source system IDs, handles history)
# MAGIC - `row_hash` for cheap change detection
# MAGIC - `valid_from` / `valid_to` / `is_current` for point-in-time joins

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customer (
# MAGIC   customer_sk  BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   customer_id  BIGINT NOT NULL,          -- natural / business key
# MAGIC   name STRING, email STRING, region STRING, segment STRING,
# MAGIC   row_hash     STRING,
# MAGIC   valid_from   DATE NOT NULL,
# MAGIC   valid_to     DATE,                     -- NULL = still current
# MAGIC   is_current   BOOLEAN NOT NULL
# MAGIC ) CLUSTER BY (customer_id);

# COMMAND ----------

TRACKED = ["name", "email", "region", "segment"]
merge_sql = scd2_merge_sql("dim_customer", "customer_changes", "customer_id", TRACKED)
print(merge_sql)

# Initial load = the same MERGE against an empty table (every row is "not matched").
(spark.table("dw_bronze_customers").withColumn("effective_date", F.lit("2025-01-01").cast("date"))
      .createOrReplaceTempView("customer_changes"))
display(spark.sql(merge_sql))

# COMMAND ----------

# Change batch on 2025-07-01: 5% of customers move region, 2% change segment, 500 new customers arrive.
changes = (spark.table("dw_bronze_customers")
           .filter((F.col("customer_id") % 20 == 0) | (F.col("customer_id") % 50 == 1))
           .withColumn("region", F.when(F.col("customer_id") % 20 == 0, F.lit("APAC")).otherwise(F.col("region")))
           .withColumn("segment", F.when(F.col("customer_id") % 50 == 1, F.lit("enterprise")).otherwise(F.col("segment"))))
new_customers = datagen.customers(spark, 100_500).filter("customer_id >= 100000")
(changes.unionByName(new_customers).withColumn("effective_date", F.lit("2025-07-01").cast("date"))
        .createOrReplaceTempView("customer_changes"))

display(spark.sql(merge_sql))   # see num_updated_rows (closed versions) and num_inserted_rows

# COMMAND ----------

# Idempotency check: re-running the same batch must change nothing.
display(spark.sql(merge_sql))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_sk, region, segment, valid_from, valid_to, is_current
# MAGIC FROM dim_customer WHERE customer_id IN (0, 1, 20, 100001) ORDER BY customer_id, valid_from;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta keeps every commit: audit + time travel (great for debugging a bad load and for rollback via RESTORE)
# MAGIC DESCRIBE HISTORY dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'version 1 (initial load)' AS v, count(*) FROM dim_customer VERSION AS OF 1
# MAGIC UNION ALL SELECT 'current', count(*) FROM dim_customer;

# COMMAND ----------

# MAGIC %md ## 5. dim_product as SCD Type 1 (overwrite, no history)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_product (
# MAGIC   product_sk BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_id BIGINT NOT NULL, product_name STRING, category STRING, list_price DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC MERGE INTO dim_product t
# MAGIC USING dw_bronze_products s ON t.product_id = s.product_id
# MAGIC WHEN MATCHED AND (t.list_price <> s.list_price OR t.category <> s.category) THEN
# MAGIC   UPDATE SET t.product_name = s.product_name, t.category = s.category, t.list_price = s.list_price
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (product_id, product_name, category, list_price) VALUES (s.product_id, s.product_name, s.category, s.list_price);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. fact_sales: point-in-time surrogate key lookup
# MAGIC Grain = one order line. Each sale must point at the customer version **valid on the order date**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_sales
# MAGIC CLUSTER BY (date_sk, customer_sk) AS
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_sk,
# MAGIC   c.customer_sk,
# MAGIC   p.product_sk,
# MAGIC   o.quantity,
# MAGIC   CAST(o.amount AS DECIMAL(12,2)) AS amount,
# MAGIC   o.status
# MAGIC FROM dw_silver_orders o
# MAGIC JOIN dim_customer c
# MAGIC   ON  o.customer_id = c.customer_id
# MAGIC   AND o.order_date >= c.valid_from
# MAGIC   AND (c.valid_to IS NULL OR o.order_date < c.valid_to)    -- half-open interval [valid_from, valid_to)
# MAGIC JOIN dim_product p ON o.product_id = p.product_id;

# COMMAND ----------

# Reconciliation: every silver row landed exactly once (no fan-out from the range join, no drops).
display(spark.sql("""
  SELECT (SELECT count(*) FROM dw_silver_orders) AS silver_rows,
         (SELECT count(*) FROM fact_sales)       AS fact_rows,
         (SELECT count(DISTINCT order_id) FROM fact_sales) AS distinct_orders
"""))

# COMMAND ----------

for stmt in [
    "ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk) RELY",
    "ALTER TABLE dim_product ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_sk) RELY",
    "ALTER TABLE fact_sales ADD CONSTRAINT fk_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer",
    "ALTER TABLE fact_sales ADD CONSTRAINT fk_product FOREIGN KEY (product_sk) REFERENCES dim_product",
    "ALTER TABLE fact_sales ADD CONSTRAINT fk_date FOREIGN KEY (date_sk) REFERENCES dim_date",
    "ANALYZE TABLE fact_sales COMPUTE STATISTICS FOR ALL COLUMNS",
    "ANALYZE TABLE dim_customer COMPUTE STATISTICS FOR ALL COLUMNS",
]:
    try_sql(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "PK/FK constraints in Unity Catalog are *informational* - not enforced - but with `RELY` the
# MAGIC optimizer can use them, e.g. to eliminate an unnecessary join or simplify DISTINCT. They also document the model
# MAGIC for BI tools and Genie. CHECK and NOT NULL *are* enforced. `ANALYZE` feeds the cost-based optimizer with row counts
# MAGIC and column stats so it picks the right join order and strategy; predictive optimization can run it for you."

# COMMAND ----------

# MAGIC %md ## 7. Analytical queries you should be fluent in

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Why SCD2 matters: revenue by region as it WAS at sale time vs restated to today's region.
# MAGIC SELECT d.month,
# MAGIC        c_hist.region AS region_at_sale_time,
# MAGIC        c_now.region  AS region_today,
# MAGIC        sum(f.amount) AS revenue
# MAGIC FROM fact_sales f
# MAGIC JOIN dim_date d          ON f.date_sk = d.date_sk
# MAGIC JOIN dim_customer c_hist ON f.customer_sk = c_hist.customer_sk
# MAGIC JOIN dim_customer c_now  ON c_hist.customer_id = c_now.customer_id AND c_now.is_current
# MAGIC WHERE d.year = 2025 AND c_hist.region <> c_now.region
# MAGIC GROUP BY ALL ORDER BY d.month LIMIT 24;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Subtotals in one pass: ROLLUP / GROUPING SETS (classic BI cube queries)
# MAGIC SELECT d.quarter, p.category, sum(f.amount) AS revenue, grouping_id() AS level
# MAGIC FROM fact_sales f JOIN dim_date d USING (date_sk) JOIN dim_product p USING (product_sk)
# MAGIC GROUP BY ROLLUP (d.quarter, p.category)
# MAGIC ORDER BY d.quarter NULLS LAST, p.category NULLS LAST;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 3 customers per region with QUALIFY (filter on a window function without a subquery)
# MAGIC SELECT c.region, c.customer_id, sum(f.amount) AS revenue,
# MAGIC        rank() OVER (PARTITION BY c.region ORDER BY sum(f.amount) DESC) AS rnk
# MAGIC FROM fact_sales f JOIN dim_customer c USING (customer_sk)
# MAGIC GROUP BY c.region, c.customer_id
# MAGIC QUALIFY rnk <= 3
# MAGIC ORDER BY c.region, rnk;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running total + month-over-month growth (window frames, LAG)
# MAGIC WITH m AS (
# MAGIC   SELECT d.year, d.month, sum(f.amount) AS revenue
# MAGIC   FROM fact_sales f JOIN dim_date d USING (date_sk) GROUP BY ALL
# MAGIC )
# MAGIC SELECT *, sum(revenue) OVER (PARTITION BY year ORDER BY month ROWS UNBOUNDED PRECEDING) AS ytd_revenue,
# MAGIC        round(100 * (revenue / lag(revenue) OVER (ORDER BY year, month) - 1), 2) AS mom_growth_pct
# MAGIC FROM m ORDER BY year, month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold serving layer: aggregates & semantic objects
# MAGIC Materialized views keep a pre-aggregated result incrementally refreshed (needs serverless / a SQL warehouse;
# MAGIC may be unavailable on some tiers - the cell reports it).

# COMMAND ----------

try_sql("""
CREATE OR REPLACE MATERIALIZED VIEW gold_daily_sales AS
SELECT f.date_sk, c.region, p.category, sum(f.amount) AS revenue, count(*) AS order_lines
FROM fact_sales f JOIN dim_customer c USING (customer_sk) JOIN dim_product p USING (product_sk)
GROUP BY ALL
""")

# Plain view fallback: a governed semantic layer object, no storage.
spark.sql("""
CREATE OR REPLACE VIEW v_sales AS
SELECT f.*, d.full_date, d.year, d.month, c.customer_id, c.region, c.segment, p.category
FROM fact_sales f JOIN dim_date d USING (date_sk) JOIN dim_customer c USING (customer_sk) JOIN dim_product p USING (product_sk)
""")
spark.sql("COMMENT ON TABLE fact_sales IS 'Order-line grain sales fact. One row per order_id. Joins to dim_customer on point-in-time SK.'")
display(spark.sql("SELECT region, sum(amount) AS revenue FROM v_sales GROUP BY region ORDER BY revenue DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Stretch exercises (do these without looking anything up)
# MAGIC 1. Add a **late-arriving change**: a region change effective 2025-04-01 that arrives *after* the 07-01 batch. What breaks in the MERGE? How does `AUTO CDC ... SEQUENCE BY` solve it?
# MAGIC 2. Make the fact load **incremental**: only new `dw_silver_orders` since the last run (hint: Delta Change Data Feed, `table_changes()`, or a watermark column) and MERGE into `fact_sales`.
# MAGIC 3. Handle an **early-arriving fact** (order for a customer not yet in dim_customer): insert an "inferred member" placeholder row and update it later.
# MAGIC 4. Write the query that proves `dim_customer` has **no overlapping validity windows** per customer (hint: `lead(valid_from)` vs `valid_to`). *(This is merge_intervals!)*
# MAGIC 5. Model a **many-to-many** (customer <-> marketing campaign) with a bridge table and explain double-counting risk.
# MAGIC 6. Rewrite `fact_sales` for a **periodic snapshot** (daily inventory) vs this transactional fact - when is each right?
