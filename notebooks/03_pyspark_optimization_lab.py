# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - PySpark optimization lab
# MAGIC Each section: **run it -> read the plan -> say the talk track.** Interviewers care that you can
# MAGIC *diagnose* (Spark UI / query profile / `explain`) before you *prescribe*.
# MAGIC
# MAGIC Diagnosis order to memorize: **Spill, Skew, Shuffle, Storage (small files), Serialization (UDFs)** - "the 5 S's".
# MAGIC
# MAGIC > Free Edition = serverless. AQE, Photon and most configs are on and locked; `df.cache()` and RDDs aren't available.
# MAGIC > Where that matters, the cell says what you'd do on classic compute - which is itself a good interview answer.

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)

from pyspark.sql import functions as F
from spark_lab import datagen, techniques as T
from spark_lab.utils import force, timed, plan_has, plan_string, try_set_conf, get_conf, partition_sizes

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
dbutils.widgets.text("n_orders", "3000000")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
N_ORDERS = int(dbutils.widgets.get("n_orders"))
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

for k, v in get_conf(spark, ["spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
                             "spark.sql.autoBroadcastJoinThreshold", "spark.databricks.photon.enabled"]).items():
    print(f"{k:<45} {v}")

# COMMAND ----------

# MAGIC %md ## 0. Build lab tables (Delta, managed in Unity Catalog)

# COMMAND ----------

datagen.customers(spark, 100_000).write.mode("overwrite").saveAsTable("lab_customers")
datagen.products(spark, 5_000).write.mode("overwrite").saveAsTable("lab_products")
datagen.orders(spark, N_ORDERS).write.mode("overwrite").saveAsTable("lab_orders")
datagen.orders(spark, N_ORDERS, hot_customer_fraction=0.4).write.mode("overwrite").saveAsTable("lab_orders_skewed")

orders, customers, products = spark.table("lab_orders"), spark.table("lab_customers"), spark.table("lab_products")
skewed = spark.table("lab_orders_skewed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reading a physical plan
# MAGIC Look for: `FileScan ... PushedFilters / PartitionFilters`, `Exchange` (= shuffle), `BroadcastExchange`,
# MAGIC `SortMergeJoin` vs `BroadcastHashJoin`, `HashAggregate` (partial + final), `Photon*` operators, `AdaptiveSparkPlan`.

# COMMAND ----------

q = (orders.filter(F.col("order_date") >= "2025-06-01")
           .join(customers, "customer_id")
           .groupBy("region").agg(F.sum("amount").alias("revenue")))
print(plan_string(q))

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "I read it bottom-up. The scan shows the pushed filter, so we only read the needed row groups.
# MAGIC Partial HashAggregate runs *before* the Exchange, so we shuffle one row per region per partition, not every order.
# MAGIC The join to customers is a broadcast because the dimension is small - no shuffle of the fact table."

# COMMAND ----------

# MAGIC %md ## 2. Join strategies: broadcast vs sort-merge

# COMMAND ----------

big_dim = datagen.customers(spark, 2_000_000)   # pretend this is too big to broadcast automatically

smj = orders.join(big_dim.hint("merge"), "customer_id")
bhj = orders.join(F.broadcast(customers), "customer_id")
print("merge hint :", plan_has(smj, "SortMergeJoin", "BroadcastHashJoin"))
print("broadcast  :", plan_has(bhj, "SortMergeJoin", "BroadcastHashJoin"))

timed("sort-merge join (shuffle both sides)", lambda: force(orders.join(customers.hint("merge"), "customer_id")), runs=2)
timed("broadcast hash join (ship small side)", lambda: force(orders.join(F.broadcast(customers), "customer_id")), runs=2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "Broadcast copies the small side to every executor - O(n+m), no shuffle of the big side.
# MAGIC Threshold is `autoBroadcastJoinThreshold` (10MB default; AQE can convert at runtime using real sizes).
# MAGIC Risk: broadcasting something large OOMs the driver/executors. Other hints: `shuffle_hash` (skip the sort when
# MAGIC one side fits per-partition), `merge`. Photon prefers hash joins."

# COMMAND ----------

# MAGIC %md ## 3. Data skew: detect, then fix

# COMMAND ----------

display(T.skew_report(skewed, "customer_id"))

# COMMAND ----------

display(partition_sizes(skewed.repartition(64, "customer_id")).limit(10))  # one partition holds ~40% of rows

# COMMAND ----------

base = lambda: force(skewed.join(big_dim.hint("merge"), "customer_id").groupBy("region").agg(F.sum("amount")))
salt = lambda: force(T.salted_join(skewed, big_dim, "customer_id", buckets=16).groupBy("region").agg(F.sum("amount")))
timed("skewed sort-merge join (AQE skew handling on)", base, runs=2)
timed("salted join (16 buckets)", salt, runs=2)
timed("two-phase aggregation on hot key", lambda: force(T.two_phase_agg(skewed, "customer_id", "amount")), runs=2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "First check the Spark UI: one task running 10-100x longer than the median, high shuffle-read
# MAGIC for that task, spill. Fix ladder: (1) is the hot key junk - NULLs or 'unknown'? filter it or handle separately;
# MAGIC (2) broadcast the other side if it's small; (3) AQE skew join (`spark.sql.adaptive.skewJoin.enabled`) splits big
# MAGIC partitions automatically; (4) salting when AQE isn't enough. Salting costs a N-x copy of the other side, so
# MAGIC salt only the hot keys in production." (On serverless AQE is already on - so the baseline is partly mitigated.)

# COMMAND ----------

# MAGIC %md ## 4. Shuffle partitions

# COMMAND ----------

for n in ["8", "200", "2000"]:
    if try_set_conf(spark, "spark.sql.shuffle.partitions", n):
        timed(f"groupBy with shuffle.partitions={n}",
              lambda: force(orders.groupBy("customer_id").agg(F.sum("amount"))), runs=2)
try_set_conf(spark, "spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "Target ~100-200MB per shuffle partition. Too few -> spill and long tasks; too many -> scheduler
# MAGIC overhead and tiny files on write. AQE coalesces small partitions after the shuffle, so I set it high-ish and
# MAGIC let AQE shrink it; on Databricks `auto` does that for me."

# COMMAND ----------

# MAGIC %md ## 5. Serialization: Python UDF vs pandas UDF vs native functions

# COMMAND ----------

oc = orders.join(F.broadcast(customers.select("customer_id", "region")), "customer_id")
py_udf, pd_udf = T.add_tax_python_udf(), T.add_tax_pandas_udf()
timed("Python UDF (row at a time, pickled)", lambda: force(oc.withColumn("t", py_udf("amount", "region"))), runs=2)
timed("pandas UDF (Arrow batches)", lambda: force(oc.withColumn("t", pd_udf("amount", "region"))), runs=2)
timed("native expressions (JVM / Photon)", lambda: force(oc.withColumn("t", T.add_tax_native())), runs=2)
print(plan_has(oc.withColumn("t", py_udf("amount", "region")), "BatchEvalPython", "ArrowEvalPython"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "UDFs are a black box to Catalyst: no predicate pushdown through them, no codegen, no Photon,
# MAGIC and each row crosses the JVM-Python boundary. Order of preference: built-in functions -> SQL higher-order
# MAGIC functions (`transform`, `filter`, `aggregate`) -> pandas UDF / `applyInPandas` -> Python UDF last."

# COMMAND ----------

# MAGIC %md ## 6. Dedupe "latest row per key": window vs self-join vs max_by

# COMMAND ----------

raw = datagen.raw_orders_with_dupes(spark, 1_000_000)
timed("self-join on max(ts)  (2 shuffles)", lambda: force(T.latest_per_key_selfjoin(raw, "order_id", "ingest_ts")), runs=2)
timed("row_number() window   (1 shuffle + sort)", lambda: force(T.latest_per_key_window(raw, "order_id", "ingest_ts")), runs=2)
timed("max_by(struct)        (hash agg, no sort)", lambda: force(T.latest_per_key_maxstruct(raw, "order_id", "ingest_ts")), runs=2)

# COMMAND ----------

# MAGIC %md ## 7. Storage layout: small files, data skipping, liquid clustering

# COMMAND ----------

# Create the classic small-files problem: many tiny appends.
spark.sql("DROP TABLE IF EXISTS lab_small_files")
for i in range(20):
    orders.filter(F.col("order_id") % 20 == i).limit(20_000).write.mode("append").saveAsTable("lab_small_files")
display(spark.sql("DESCRIBE DETAIL lab_small_files").select("numFiles", "sizeInBytes"))
spark.sql("OPTIMIZE lab_small_files")
display(spark.sql("DESCRIBE DETAIL lab_small_files").select("numFiles", "sizeInBytes"))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lab_orders_clustered")
spark.sql("CREATE TABLE lab_orders_clustered CLUSTER BY (customer_id, order_date) AS SELECT * FROM lab_orders")
spark.sql("OPTIMIZE lab_orders_clustered")

point = "customer_id = 4242 AND order_date BETWEEN '2025-03-01' AND '2025-03-31'"
timed("point query - unclustered", lambda: spark.table("lab_orders").filter(point).collect(), runs=3)
timed("point query - liquid clustered", lambda: spark.table("lab_orders_clustered").filter(point).collect(), runs=3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Talk track:** "Delta stores min/max stats per file for the first 32 columns. A filter skips any file whose
# MAGIC [min,max] can't contain the value - that's a range check, like binary search over file stats. Skipping only
# MAGIC works if data is *clustered* on the filter columns. Liquid clustering replaces Hive partitioning + Z-ORDER:
# MAGIC keys can change without a rewrite, it's incremental, and it avoids the small-files trap of over-partitioning.
# MAGIC Rule of thumb for Hive-style partitions: only on low-cardinality columns with >=1GB per partition.
# MAGIC Check the query profile for 'files pruned'. Predictive optimization runs OPTIMIZE/VACUUM/ANALYZE automatically."

# COMMAND ----------

# MAGIC %md ## 8. Caching on serverless vs classic

# COMMAND ----------

try:
    orders.cache().count()
    print("df.cache() worked - you're on classic compute")
except Exception as e:
    print(f"df.cache() not supported here: {type(e).__name__}")
    print("Serverless alternative: materialize an intermediate result as a Delta table (or temp view over it);")
    print("Databricks also has automatic disk caching of remote files on SSD and query result caching in SQL warehouses.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cheat sheet: symptom -> cause -> fix
# MAGIC | Symptom (Spark UI / query profile) | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | One task 10x slower than median | Skew | filter junk keys, broadcast, AQE skew join, salting |
# MAGIC | "Spill (disk)" in stage metrics | partitions too big for memory | more shuffle partitions, bigger memory per core, fix skew |
# MAGIC | Huge shuffle read/write | wide joins/aggs on big data | broadcast small side, pre-aggregate, filter/project early |
# MAGIC | Thousands of tiny files, slow listing | small appends / over-partitioning | OPTIMIZE, optimized writes/auto compaction, liquid clustering |
# MAGIC | Full scan despite a filter | no data skipping | cluster by the filter column, avoid functions on filter columns |
# MAGIC | Low CPU, long time in Python | Python UDFs | native functions, pandas UDFs |
# MAGIC | Driver OOM | `collect()`, `toPandas()`, huge broadcast | aggregate first, write to table, lower broadcast threshold |
# MAGIC | Row count explodes after join | many-to-many / duplicate keys | check key uniqueness before joining |
