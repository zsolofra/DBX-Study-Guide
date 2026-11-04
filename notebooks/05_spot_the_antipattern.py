# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Brain teasers: "Why is this code not optimal?"
# MAGIC Every snippet below **works** and returns the right answer. Each one is slow, fragile, or wasteful,
# MAGIC usually because it bounces between **Python on the driver** and **Spark on the executors** for no reason.
# MAGIC
# MAGIC **For each teaser, answer out loud (2 minutes max):**
# MAGIC 1. **Where does each line run?** Driver (one machine, Python) or executors (distributed, JVM/Photon)?
# MAGIC 2. **What's the hidden cost?** Data pulled to the driver, rows crossing JVM<->Python, extra Spark jobs, extra shuffles, plan bloat, small files.
# MAGIC 3. **The fix**, in one line of PySpark or SQL.
# MAGIC 4. **When would the original actually be OK?** (Interviewers love this nuance.)
# MAGIC
# MAGIC Two of the teasers are **traps**: the code is fine. Spot which ones.
# MAGIC
# MAGIC Answers: `05_spot_the_antipattern_ANSWERS`. Don't open it until you've written your diagnosis in the empty cells.
# MAGIC
# MAGIC Difficulty: (1) warm-up, (2) common in interviews, (3) senior-level nuance

# COMMAND ----------

# MAGIC %md ## Setup (small tables so the bad code still finishes)

# COMMAND ----------

import os, sys, json
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from spark_lab import datagen

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

TEASER_ROWS = 100_000
datagen.orders(spark, TEASER_ROWS, n_customers=10_000, n_products=1_000).write.mode("overwrite").saveAsTable("teaser_orders")
datagen.customers(spark, 10_000).write.mode("overwrite").saveAsTable("teaser_customers")
datagen.products(spark, 1_000).write.mode("overwrite").saveAsTable("teaser_products")

orders, customers, products = spark.table("teaser_orders"), spark.table("teaser_customers"), spark.table("teaser_products")

# COMMAND ----------

# MAGIC %md ## Teaser 1 (1) - "Revenue from delivered orders"

# COMMAND ----------

total = 0
for row in orders.collect():
    if row.status == "delivered":
        total += row.amount
print(round(total, 2))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 2 (1) - "Spend per customer, the pandas way"

# COMMAND ----------

pdf = orders.toPandas()
summary = pdf.groupby("customer_id", as_index=False)["amount"].sum()
customer_spend = spark.createDataFrame(summary)
customer_spend.write.mode("overwrite").saveAsTable("teaser_customer_spend")

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 3 (1) - "Clean up emails"

# COMMAND ----------

@udf("string")
def clean_email(e):
    return e.strip().lower() if e is not None else None

display(customers.withColumn("email_clean", clean_email("email")).limit(5))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 4 (2) - "Revenue for each region"

# COMMAND ----------

regions = [r.region for r in customers.select("region").distinct().collect()]
result = None
for r in regions:
    df = (orders.join(customers, "customer_id")
                .filter(F.col("region") == r)
                .agg(F.sum("amount").alias("revenue"))
                .withColumn("region", F.lit(r)))
    result = df if result is None else result.union(df)
display(result)

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 5 (2) - "Orders from VIP customers"

# COMMAND ----------

vip = customers.filter("segment = 'enterprise'")
vip_ids = [r.customer_id for r in vip.select("customer_id").collect()]
vip_orders = orders.filter(F.col("customer_id").isin(vip_ids))
print(vip_orders.count())

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 6 (2) - "Look up the list price for each order"

# COMMAND ----------

price_lookup = {r.product_id: r.list_price for r in products.collect()}

@udf("double")
def get_price(pid):
    return price_lookup.get(pid)

display(orders.withColumn("list_price", get_price("product_id")).limit(5))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 7 (1) - "Trim every string column"

# COMMAND ----------

df = customers
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, F.trim(F.col(c)))
for c in ["name", "email", "region", "segment"]:
    df = df.withColumn(c + "_len", F.length(F.col(c)))
display(df.limit(5))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 8 (1) - "Only write if there's new data"

# COMMAND ----------

new_rows = orders.filter("order_date >= '2025-12-01'").join(customers, "customer_id")
if new_rows.count() > 0:
    print(f"writing {new_rows.count()} rows")
    new_rows.write.mode("overwrite").saveAsTable("teaser_december")

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 9 (2) - "Load a few new customers"

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE teaser_target (customer_id BIGINT, name STRING, region STRING)")
incoming = [{"customer_id": 900_000 + i, "name": f"new_{i}", "region": "NA"} for i in range(25)]
for rec in incoming:
    spark.sql(f"INSERT INTO teaser_target VALUES ({rec['customer_id']}, '{rec['name']}', '{rec['region']}')")
display(spark.sql("DESCRIBE DETAIL teaser_target").select("numFiles"))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 10 (2) - "Find the incremental-load watermark"

# COMMAND ----------

dates = [r.order_date for r in orders.select("order_date").collect()]
watermark = max(dates)
print("last loaded date:", watermark)

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 11 (2) - "Running total of revenue per customer"

# COMMAND ----------

rows = orders.select("customer_id", "order_date", "order_id", "amount").orderBy("customer_id", "order_date", "order_id").collect()
out, running, prev = [], 0.0, None
for r in rows:
    running = r.amount if r.customer_id != prev else running + r.amount
    prev = r.customer_id
    out.append((r.customer_id, r.order_date, r.order_id, r.amount, running))
running_df = spark.createDataFrame(out, "customer_id long, order_date date, order_id long, amount double, running_total double")
display(running_df.limit(10))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 12 (2) - "Parse the JSON payload"

# COMMAND ----------

events = orders.select("order_id", F.to_json(F.struct("amount", "status", "quantity")).alias("payload"))

@udf("double")
def payload_amount(p):
    return json.loads(p)["amount"]

display(events.withColumn("amount", payload_amount("payload")).limit(5))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 13 (3) - "Build a report, check it, save it"

# COMMAND ----------

report = (orders.join(customers, "customer_id").join(products, "product_id")
                .groupBy("region", "category").agg(F.sum("amount").alias("revenue")))
display(report)
print("rows:", report.count())
print("total revenue:", report.agg(F.sum("revenue")).first()[0])
report.write.mode("overwrite").saveAsTable("teaser_report")

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 14 (2) - "Filter by a date the user picked"

# COMMAND ----------

dbutils.widgets.text("start_date", "2025-11-01")
start = dbutils.widgets.get("start_date")
display(spark.sql(f"SELECT status, count(*) AS n FROM teaser_orders WHERE order_date >= '{start}' GROUP BY status"))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 15 (2) - "Ingest a folder of daily files"

# COMMAND ----------

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/teaser_files"
try:
    spark.sql("CREATE VOLUME IF NOT EXISTS teaser_files")
    orders.repartition(12).write.mode("overwrite").json(f"{VOL}/orders_json")

    combined = None
    for f in dbutils.fs.ls(f"{VOL}/orders_json"):
        if f.name.endswith(".json"):
            part = spark.read.json(f.path)
            combined = part if combined is None else combined.unionByName(part)
    print(combined.count())
except Exception as e:
    print(f"[skipped - volumes unavailable] {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 16 (3) - "Discount column with a pandas UDF - that's the fast kind, right?"

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def discounted(amount: pd.Series, qty: pd.Series) -> pd.Series:
    return amount * (1 - (qty >= 3) * 0.1)

display(orders.withColumn("net", discounted("amount", "quantity")).limit(5))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 17 (3) - TRAP? "Drive a loop from a small config result"

# COMMAND ----------

per_region = customers.groupBy("region").count().collect()     # 4 rows
for r in per_region:
    print(f"{r.region:<6} {r['count']:>6} customers")

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 18 (3) - TRAP? "Model a sample on the driver"

# COMMAND ----------

sample_pdf = (orders.join(customers, "customer_id")
                    .groupBy("segment", "region").agg(F.avg("amount").alias("avg_amount"), F.count("*").alias("n"))
                    .toPandas())
print(sample_pdf.pivot(index="segment", columns="region", values="avg_amount").round(2))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 19 (3) - "Salt the hot key"

# COMMAND ----------

import random

@udf("int")
def random_salt():
    return random.randint(0, 15)

salted = orders.withColumn("salt", random_salt())
display(salted.groupBy("salt").count().orderBy("salt"))

# COMMAND ----------

# MAGIC %md **Your diagnosis:**

# COMMAND ----------

# MAGIC %md ## Teaser 20 (3) - "Process each customer's orders"

# COMMAND ----------

def score_customer(cust_id):
    rows = orders.filter(F.col("customer_id") == cust_id).collect()
    return cust_id, sum(r.amount for r in rows) / max(len(rows), 1)

some_customers = [r.customer_id for r in customers.limit(20).collect()]
scores = [score_customer(c) for c in some_customers]     # imagine this for all 10,000
print(scores[:5])

# COMMAND ----------

# MAGIC %md **Your diagnosis:**
