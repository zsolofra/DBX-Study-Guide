# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - ANSWERS: "Why is this code not optimal?"
# MAGIC Each answer gives the **problem**, the **hidden cost**, the **fix** (runnable, with a timing comparison where it's
# MAGIC interesting), and **when the original is OK**.
# MAGIC
# MAGIC ### The one rule behind almost every teaser
# MAGIC > **Keep data on the executors, and keep logic in Spark expressions.** Only bring *small, already-aggregated*
# MAGIC > results to the driver. Each `collect()` / `toPandas()` / Python UDF / Python loop over Spark calls is a
# MAGIC > boundary crossing, and you should be able to say why it's worth it.

# COMMAND ----------

import os, sys, json
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)

from pyspark.sql import Window, functions as F
from pyspark.sql.functions import udf
from spark_lab import datagen
from spark_lab.utils import timed, force, plan_has

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

if not spark.catalog.tableExists("teaser_orders"):
    datagen.orders(spark, 100_000, n_customers=10_000, n_products=1_000).write.mode("overwrite").saveAsTable("teaser_orders")
    datagen.customers(spark, 10_000).write.mode("overwrite").saveAsTable("teaser_customers")
    datagen.products(spark, 1_000).write.mode("overwrite").saveAsTable("teaser_products")
orders, customers, products = spark.table("teaser_orders"), spark.table("teaser_customers"), spark.table("teaser_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 1 - `collect()` then a Python loop to sum
# MAGIC - **Problem:** ships *every row and every column* to the driver, then filters and sums in single-threaded Python.
# MAGIC - **Cost:** network + driver memory scale with the table (driver OOM at real volumes). No pushdown: the `status` filter runs after the transfer.
# MAGIC - **Fix:** filter + aggregate in Spark. Only one number comes back.
# MAGIC - **OK when:** never for a sum. Collect only *results*.

# COMMAND ----------

def bad1():
    total = 0
    for row in orders.collect():
        if row.status == "delivered":
            total += row.amount
    return total

good1 = lambda: orders.filter(F.col("status") == "delivered").agg(F.sum("amount")).first()[0]
timed("T1 bad : collect + python loop", bad1)
timed("T1 good: filter + agg", good1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 2 - `toPandas()` -> pandas groupby -> `createDataFrame()`
# MAGIC - **Problem:** distributed -> single machine -> distributed again. Two full serializations of the data.
# MAGIC - **Cost:** the driver must hold the whole table in pandas (roughly 2-5x the on-disk size). The groupby is single-threaded, and the round trip back creates a new plan with no lineage to the source.
# MAGIC - **Fix:** `groupBy().agg()`. If you *need* pandas logic per group, use `groupBy().applyInPandas()`, which runs pandas on the executors one group at a time. Or use the pandas API on Spark (`pyspark.pandas`).
# MAGIC - **OK when:** the input is already tiny (an aggregated result, a sample for a chart).

# COMMAND ----------

timed("T2 bad : toPandas -> groupby -> createDataFrame",
      lambda: spark.createDataFrame(orders.toPandas().groupby("customer_id", as_index=False)["amount"].sum()).collect())
timed("T2 good: groupBy().agg()", lambda: orders.groupBy("customer_id").agg(F.sum("amount").alias("amount")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 3 - Python UDF for something Spark already has
# MAGIC - **Problem:** `strip().lower()` reimplemented as a row-at-a-time Python UDF.
# MAGIC - **Cost:** every row is pickled JVM -> Python worker -> JVM. A UDF is a black box to Catalyst: no Photon, no codegen, no filter pushdown through it.
# MAGIC - **Fix:** `F.lower(F.trim("email"))`. Check `pyspark.sql.functions` before writing any UDF; there are hundreds of built-ins (regex, dates, JSON, arrays, maps, and higher-order `transform`/`filter`/`aggregate`).
# MAGIC - **OK when:** the logic truly isn't expressible in built-ins (calling a Python library), and then prefer a pandas UDF.

# COMMAND ----------

@udf("string")
def clean_email(e):
    return e.strip().lower() if e is not None else None

timed("T3 bad : python UDF", lambda: force(customers.withColumn("e", clean_email("email"))))
timed("T3 good: lower(trim())", lambda: force(customers.withColumn("e", F.lower(F.trim("email")))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 4 - Python loop over distinct values, one Spark job each, then `union`
# MAGIC - **Problem:** this is a hand-rolled `GROUP BY`. With N regions it runs N joins plus 1 collect, and the unioned plan has N copies of the join.
# MAGIC - **Cost:** N times the scan and join work. Planning time grows with N. With 500 values instead of 4, this takes minutes instead of seconds.
# MAGIC - **Fix:** `orders.join(customers, "customer_id").groupBy("region").agg(F.sum("amount"))`. That's one pass and one shuffle.
# MAGIC - **OK when:** each iteration does genuinely different work (different target tables or different logic) and N is small. Even then, consider a Jobs *for-each task*.

# COMMAND ----------

def bad4():
    result = None
    for r in [x.region for x in customers.select("region").distinct().collect()]:
        df = (orders.join(customers, "customer_id").filter(F.col("region") == r)
                    .agg(F.sum("amount").alias("revenue")).withColumn("region", F.lit(r)))
        result = df if result is None else result.union(df)
    return result.collect()

timed("T4 bad : loop per region + union", bad4)
timed("T4 good: groupBy region", lambda: orders.join(customers, "customer_id").groupBy("region").agg(F.sum("amount")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 5 - collect IDs into a Python list, then `isin(huge_list)`
# MAGIC - **Problem:** turns a join into a giant literal `IN (...)` list embedded in the query plan.
# MAGIC - **Cost:** driver round trip. With 100k+ IDs the plan becomes megabytes: slow planning, a big request to the server (Spark Connect), and possible plan-size errors.
# MAGIC - **Fix:** a **left semi join**, which keeps orders that have a match and adds no columns. Spark broadcasts the small side automatically.
# MAGIC - **OK when:** the list is small and static (`isin("NA", "EMEA")`).

# COMMAND ----------

vip = customers.filter("segment = 'enterprise'")
timed("T5 bad : collect ids + isin", lambda: orders.filter(F.col("customer_id").isin([r.customer_id for r in vip.select("customer_id").collect()])).count())
timed("T5 good: left_semi join", lambda: orders.join(vip.select("customer_id"), "customer_id", "left_semi").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 6 - Python dict lookup inside a UDF
# MAGIC - **Problem:** a hand-built broadcast join. The dict is collected to the driver and pickled into the UDF closure, so it gets shipped to every task, and then every row crosses into Python.
# MAGIC - **Cost:** UDF serialization per row, plus the closure size per task. The optimizer can't see it's a join.
# MAGIC - **Fix:** `orders.join(F.broadcast(products.select("product_id", "list_price")), "product_id", "left")`.
# MAGIC - **OK when:** basically never. A broadcast join is the same idea done properly.

# COMMAND ----------

price_lookup = {r.product_id: r.list_price for r in products.collect()}

@udf("double")
def get_price(pid):
    return price_lookup.get(pid)

timed("T6 bad : dict in UDF", lambda: force(orders.withColumn("list_price", get_price("product_id"))))
timed("T6 good: broadcast join", lambda: force(orders.join(F.broadcast(products.select("product_id", "list_price")), "product_id", "left")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 7 - `withColumn` in a loop
# MAGIC - **Problem:** each `withColumn` adds a new projection to the plan, and the analyzer re-processes the growing plan every time.
# MAGIC - **Cost:** trivial for 8 columns, but with hundreds of columns (common in wide bronze tables) planning alone can take minutes, sometimes ending in a StackOverflow. The analysis cost grows roughly O(n^2).
# MAGIC - **Fix:** build all expressions, then make **one** `select` (or `withColumns({...})` in Spark 3.3+).
# MAGIC - **OK when:** a handful of columns. Mention the scaling issue anyway; it shows you know the planner.

# COMMAND ----------

trimmed = [F.trim(F.col(c)).alias(c) if t == "string" else F.col(c) for c, t in customers.dtypes]
lengths = [F.length(F.trim(F.col(c))).alias(c + "_len") for c in ["name", "email", "region", "segment"]]
display(customers.select(*trimmed, *lengths).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 8 - `count() > 0` to check for emptiness (twice!)
# MAGIC - **Problem:** `count()` scans everything just to learn whether there's at least one row. Then it runs again for the print, and the join runs a *third* time for the write.
# MAGIC - **Cost:** three executions of the join, two of which are thrown away.
# MAGIC - **Fix:** `df.isEmpty()` (Spark 3.3+, stops at the first row). Better still, just write: writing an empty DataFrame is harmless. Get the row count afterwards from the Delta commit metrics (`DESCRIBE HISTORY` -> `operationMetrics.numOutputRows`).
# MAGIC - **OK when:** you need the exact count anyway, so compute it **once** and store it in a variable.

# COMMAND ----------

new_rows = orders.filter("order_date >= '2025-12-01'").join(customers, "customer_id")
if not new_rows.isEmpty():
    new_rows.write.mode("overwrite").saveAsTable("teaser_december")
    m = spark.sql("DESCRIBE HISTORY teaser_december LIMIT 1").select("operationMetrics").first()[0]
    print("rows written:", m.get("numOutputRows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 9 - one `INSERT` per row from a Python loop
# MAGIC - **Problem:** treats Delta like an OLTP database.
# MAGIC - **Cost:** 25 inserts mean 25 Spark jobs, 25 Delta commits (versions), and 25 tiny files. The f-string SQL is also **SQL-injectable** (try a name like `O'Brien`).
# MAGIC - **Fix:** build one DataFrame and do one write, or one `MERGE` if it's an upsert. That's 1 job, 1 commit and 1 file.
# MAGIC - **OK when:** never for data. For true row-level OLTP needs, use **Lakebase** (managed Postgres) and sync it to the lakehouse.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE teaser_target (customer_id BIGINT, name STRING, region STRING)")
incoming = [{"customer_id": 900_000 + i, "name": f"new_{i}", "region": "NA"} for i in range(25)]
spark.createDataFrame(incoming, "customer_id long, name string, region string").write.mode("append").saveAsTable("teaser_target")
display(spark.sql("DESCRIBE DETAIL teaser_target").select("numFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 10 - collect a whole column to compute `max()` in Python
# MAGIC - **Problem:** sends 100k (or 10B) values to the driver to compute one number.
# MAGIC - **Fix:** `orders.agg(F.max("order_date"))`. On Delta this can often be answered largely from **file-level min/max statistics** without reading the data at all.
# MAGIC - **Senior nuance:** for incremental loads, a hand-maintained max-date watermark misses late-arriving rows. Prefer **Structured Streaming / Auto Loader checkpoints** or **Delta Change Data Feed** (`table_changes`), which track exactly what was processed.

# COMMAND ----------

timed("T10 bad : collect + max()", lambda: max(r.order_date for r in orders.select("order_date").collect()))
timed("T10 good: agg(max)", lambda: orders.agg(F.max("order_date")).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 11 - running total computed in a Python loop
# MAGIC - **Problem:** a global sort, then collecting every row, then sequential Python, then pushing everything back with `createDataFrame`.
# MAGIC - **Cost:** the entire table passes through the driver twice, and the loop is single-threaded.
# MAGIC - **Fix:** a window function partitioned by customer. Spark parallelizes across customers, and each partition sorts locally.
# MAGIC - **Interview link:** this is prefix sums (from the DS&A section) expressed as SQL.

# COMMAND ----------

w = Window.partitionBy("customer_id").orderBy("order_date", "order_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
running = orders.select("customer_id", "order_date", "order_id", "amount").withColumn("running_total", F.sum("amount").over(w))
timed("T11 good: window function", lambda: force(running))
display(running.orderBy("customer_id", "order_date", "order_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 12 - `json.loads` inside a UDF
# MAGIC - **Problem:** Python JSON parsing per row inside a UDF.
# MAGIC - **Fix:** use native JSON functions, which run in the JVM/Photon:
# MAGIC   - one field: `F.get_json_object("payload", "$.amount")`
# MAGIC   - many fields: `F.from_json("payload", schema)`, parsing once into a struct
# MAGIC   - semi-structured at scale: the **VARIANT** type (`parse_json`), which is faster than strings and needs no fixed schema
# MAGIC - **Bonus:** the UDF crashes on a malformed row. `from_json` returns NULL instead, so you can route bad rows to quarantine.

# COMMAND ----------

events = orders.select("order_id", F.to_json(F.struct("amount", "status", "quantity")).alias("payload"))

@udf("double")
def payload_amount(p):
    return json.loads(p)["amount"]

timed("T12 bad : json.loads UDF", lambda: force(events.withColumn("amount", payload_amount("payload"))))
timed("T12 good: from_json", lambda: force(events.withColumn("p", F.from_json("payload", "amount double, status string, quantity int")).select("order_id", "p.*")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 13 - same expensive DataFrame, four actions
# MAGIC - **Problem:** `display`, `count`, the `agg`, and `write` are four separate **actions**. DataFrames are lazy, so the three-way join and aggregation is recomputed **four times**.
# MAGIC - **Fix on serverless:** **write first**, then run checks against the saved table. The checks become cheap reads, and `count()` on a Delta table can often be answered from metadata.
# MAGIC - **Fix on classic compute:** `report.cache()` (or `persist`) before the first action, and `unpersist()` afterwards.
# MAGIC - **Senior nuance:** don't cache by reflex. It uses executor memory and only pays off when the DataFrame is reused *and* expensive to recompute.

# COMMAND ----------

report = (orders.join(customers, "customer_id").join(products, "product_id")
                .groupBy("region", "category").agg(F.sum("amount").alias("revenue")))
report.write.mode("overwrite").saveAsTable("teaser_report")     # compute once
saved = spark.table("teaser_report")                            # cheap from here on
display(saved)
print("rows:", saved.count(), "| total revenue:", saved.agg(F.sum("revenue")).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 14 - f-string SQL with user input
# MAGIC - **Problem:** this one isn't a performance issue. It's **SQL injection**: a widget value of `2025-01-01' OR '1'='1` changes the query. String dates are also compared implicitly instead of being typed.
# MAGIC - **Fix:** **named parameter markers**: `spark.sql("... WHERE order_date >= :start", args={"start": ...})`. In a `%sql` cell or a SQL warehouse, use `:start_date` widget/query parameters.
# MAGIC - **Interview angle:** a Delivery Solutions Architect reviews customer code, so flagging security issues alongside performance is a plus.

# COMMAND ----------

dbutils.widgets.text("start_date", "2025-11-01")
display(spark.sql(
    "SELECT status, count(*) AS n FROM teaser_orders WHERE order_date >= CAST(:start AS DATE) GROUP BY status",
    args={"start": dbutils.widgets.get("start_date")},
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 15 - list files in Python, read each one, union
# MAGIC - **Problem:** N separate reads. Each `spark.read.json` without a schema **scans the file to infer the schema**, so every file is read twice. The union plan grows with N.
# MAGIC - **Fix:** one read of the directory **with an explicit schema**: `spark.read.schema(s).json(dir)`. Spark lists the files and parallelizes across them.
# MAGIC - **Production fix:** **Auto Loader** (`cloudFiles`) processes only *new* files incrementally, tracks state in a checkpoint, and handles schema evolution. Mention it by name.

# COMMAND ----------

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/teaser_files/orders_json"
try:
    schema = "order_id long, customer_id long, product_id long, order_date date, quantity int, amount double, status string"
    print(spark.read.schema(schema).json(VOL).count())
except Exception as e:
    print(f"[run teaser 15 in the questions notebook first] {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 16 - a pandas UDF for simple arithmetic
# MAGIC - **Problem:** pandas UDFs are faster than plain UDFs, but they still convert to Arrow and send batches to a Python worker. For `amount * (1 - 0.1 * (qty >= 3))`, that's pure overhead.
# MAGIC - **Fix:** a native expression: `F.col("amount") * F.when(F.col("quantity") >= 3, 0.9).otherwise(1.0)`.
# MAGIC - **Order of preference:** built-ins > SQL higher-order functions > pandas UDF > Python UDF.
# MAGIC - **OK when:** you need a Python library (scikit-learn scoring, a custom parser, NLP).

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def discounted(amount: pd.Series, qty: pd.Series) -> pd.Series:
    return amount * (1 - (qty >= 3) * 0.1)

timed("T16 bad : pandas UDF", lambda: force(orders.withColumn("net", discounted("amount", "quantity"))))
timed("T16 good: native when()", lambda: force(orders.withColumn("net", F.col("amount") * F.when(F.col("quantity") >= 3, 0.9).otherwise(1.0))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 17 - TRAP: this code is fine
# MAGIC The aggregation happens in Spark, and only **4 rows** come back. Looping over a tiny, *bounded* result on the driver is
# MAGIC normal control-plane code. If you "fixed" this, you'd be over-engineering.
# MAGIC
# MAGIC **What to say:** "`collect()` is fine when the result size is bounded by design: after an aggregation, a `limit`, or a
# MAGIC config table. The anti-pattern is collecting *data*, not *results*."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 18 - TRAP: this is also fine
# MAGIC The join and group-by run distributed. `toPandas()` receives only about 12 rows (3 segments x 4 regions), and pandas `pivot`
# MAGIC on 12 rows is the right tool. You *could* use Spark's `.groupBy().pivot()`, but it isn't necessary.
# MAGIC
# MAGIC **Watch for the variant that isn't fine:** `toPandas()` *before* the groupBy (that's Teaser 2).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 19 - salting with Python's `random` in a UDF
# MAGIC - **Problem 1: correctness.** The UDF isn't marked non-deterministic, so Spark assumes it is deterministic and may evaluate it more than once, for example on a task retry or when the optimizer duplicates the expression. A row can get *different* salts in different places, which silently breaks a salted join.
# MAGIC - **Problem 2: speed.** It's a Python UDF for something Spark has built in.
# MAGIC - **Fix:** `(F.rand(seed=42) * 16).cast("int")`. It's native, Spark knows it's non-deterministic, and it's reproducible for the same input partitioning.
# MAGIC - **Senior nuance:** salting by a hash of another column (`F.abs(F.hash("order_id")) % 16`) is *fully* deterministic, which is the safest choice under retries.

# COMMAND ----------

display(orders.withColumn("salt", F.abs(F.hash("order_id")) % 16).groupBy("salt").count().orderBy("salt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaser 20 - one Spark query per customer (the "N+1 query" problem)
# MAGIC - **Problem:** a Python loop issues a separate filter + collect for every key. 10,000 customers means 10,000 Spark jobs, each scanning the table.
# MAGIC - **Cost:** job scheduling overhead dominates. On a big table each job is a full scan unless the table is clustered on `customer_id`. This takes hours instead of seconds.
# MAGIC - **Fix:** `groupBy("customer_id").agg(F.avg("amount"))`, which covers every customer in one pass.
# MAGIC - **If the per-key logic really is custom Python:** use `groupBy("customer_id").applyInPandas(fn, schema)`, which runs your function per group, *in parallel on the executors*.

# COMMAND ----------

def bad20():
    def score(cid):
        rows = orders.filter(F.col("customer_id") == cid).collect()
        return cid, sum(r.amount for r in rows) / max(len(rows), 1)
    return [score(r.customer_id) for r in customers.limit(20).collect()]

timed("T20 bad : 20 customers, 20 Spark jobs", bad20)
timed("T20 good: ALL 10,000 customers, 1 job", lambda: orders.groupBy("customer_id").agg(F.avg("amount")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: smells to scan for in any code review
# MAGIC | Smell | Ask yourself | Usual fix |
# MAGIC |---|---|---|
# MAGIC | `.collect()` / `.toPandas()` early in a pipeline | Is the result bounded and small? | aggregate in Spark first |
# MAGIC | `for x in ...: spark.something(x)` | Is this a hidden GROUP BY or JOIN? | `groupBy`, a join, `applyInPandas` |
# MAGIC | `@udf` | Is there a built-in? | `F.*`, higher-order functions, `from_json` |
# MAGIC | `@pandas_udf` on simple math | Does it need a Python library? | native expressions |
# MAGIC | `isin(python_list)` built from a collect | Is it really a join? | `left_semi` join |
# MAGIC | Several actions on one DataFrame | Is it being recomputed? | write once / cache |
# MAGIC | `count()` just for an if-check | Do I need the number? | `isEmpty()` |
# MAGIC | Row-by-row `INSERT` | Is this OLTP thinking? | one DataFrame write / MERGE |
# MAGIC | f-string SQL with input | Injection? | `args={...}` parameter markers |
# MAGIC | `withColumn` loop over many columns | Is the plan growing? | a single `select` / `withColumns` |
# MAGIC | Python `random` / `datetime.now()` inside a UDF | Determinism under retries? | `F.rand(seed)`, hash-based salt, `F.current_timestamp()` |
