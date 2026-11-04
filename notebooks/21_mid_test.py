# Databricks notebook source
# MAGIC %md
# MAGIC # Level 2 - MID test
# MAGIC **Rules:** about 75 minutes. No peeking. Say the pattern name *before* you code each problem. Pass mark: **85%**.
# MAGIC
# MAGIC | Section | Items |
# MAGIC |---|---|
# MAGIC | Coding | M1-M6 |
# MAGIC | Spark | MS1-MS4 |
# MAGIC | Multiple choice / diagnosis | Q1-Q8 |

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from pyspark.sql import Window, functions as F
from levels.grader import Scorecard

card = Scorecard("mid", spark)
raw_orders, orders = card.data["raw_orders"], card.data["orders"]
sales, dim_hist = card.data["sales"], card.data["dim_hist"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coding
# MAGIC ### M1 - Longest substring without repeating characters
# MAGIC Return the length. Target: O(n).

# COMMAND ----------

def length_of_longest_substring(s):
    raise NotImplementedError

card.grade("M1", length_of_longest_substring)

# COMMAND ----------

# MAGIC %md
# MAGIC ### M2 - Count subarrays that sum to k
# MAGIC `nums` may contain **negative** numbers. Count contiguous subarrays whose sum equals `k`. Target: O(n).

# COMMAND ----------

def subarray_sum_k(nums, k):
    raise NotImplementedError

card.grade("M2", subarray_sum_k)

# COMMAND ----------

# MAGIC %md
# MAGIC ### M3 - Minimum eating speed
# MAGIC `piles[i]` bananas. Each hour you pick one pile and eat up to `speed` bananas from it (if the pile has fewer, you finish it and wait out the hour).
# MAGIC Return the **minimum integer speed** that finishes all piles within `h` hours (`h >= len(piles)`). Target: O(n log max(piles)).

# COMMAND ----------

def min_eating_speed(piles, h):
    raise NotImplementedError

card.grade("M3", min_eating_speed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### M4 - Course order
# MAGIC Courses are `0..n-1`. Each `[course, prereq]` means prereq must come first. Return **any** valid order, or `[]` if impossible.
# MAGIC Target: O(V + E).

# COMMAND ----------

def course_order(n, prereqs):
    raise NotImplementedError

card.grade("M4", course_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ### M5 - Top-k frequent words
# MAGIC Return the `k` most frequent words, sorted by frequency (highest first), with ties broken **alphabetically**.

# COMMAND ----------

def top_k_frequent_words(words, k):
    raise NotImplementedError

card.grade("M5", top_k_frequent_words)

# COMMAND ----------

# MAGIC %md
# MAGIC ### M6 - Merge intervals
# MAGIC Merge overlapping `[start, end]` intervals (touching counts as overlapping: `[1,4],[4,5]` becomes `[1,5]`). Input may be unsorted.
# MAGIC Return them sorted by start.

# COMMAND ----------

def merge_intervals(intervals):
    raise NotImplementedError

card.grade("M6", merge_intervals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark
# MAGIC Return DataFrames with **exactly** the listed columns. Order doesn't matter. Use DataFrame or SQL (`createOrReplaceTempView` + `spark.sql`).
# MAGIC
# MAGIC ### MS1 - Latest status per order
# MAGIC `raw_orders(order_id, status, ingested_at)` contains replays. Keep the **most recently ingested** row per order.
# MAGIC Columns: `order_id`, `status`.

# COMMAND ----------

ms1 = None

card.grade_df("MS1", ms1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MS2 - Each customer's 3 most recent orders
# MAGIC From `orders(order_id, customer_id, order_date, amount)`, rank orders per customer by `order_date` descending, breaking ties by **higher `order_id` first**.
# MAGIC Keep rank <= 3. Columns: `customer_id`, `order_id`, `rn` (1 = most recent). Every rank must be unique.

# COMMAND ----------

ms2 = None

card.grade_df("MS2", ms2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MS3 - Month-over-month revenue growth
# MAGIC Columns: `month` (string `yyyy-MM`), `revenue` (sum of amount, rounded to 2), `mom_pct` = `(revenue / previous month's revenue - 1) * 100`
# MAGIC rounded to 2, NULL for the first month.

# COMMAND ----------

ms3 = None

card.grade_df("MS3", ms3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MS4 - Region at time of sale (SCD2 point-in-time join)
# MAGIC `sales(sale_id, customer_id, sale_date)`, `dim_hist(customer_id, region, valid_from, valid_to)`, where `valid_to` is **exclusive** and NULL means current.
# MAGIC Columns: `sale_id`, `region`. Keep **every** sale exactly once (region NULL if there's no match).

# COMMAND ----------

ms4 = None

card.grade_df("MS4", ms4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple choice / diagnosis
# MAGIC **Q1.** The plan shows `BroadcastExchange` under the dimension side and `BroadcastHashJoin`. This means: a) both sides are shuffled by the join key
# MAGIC b) the dimension is copied to every executor and the fact side isn't shuffled c) the join runs on the driver d) the dimension is cached on disk
# MAGIC
# MAGIC **Q2.** A stage has 200 tasks: 199 finish in about 5 s, one takes 25 minutes with a huge shuffle read. Most likely cause: a) too few shuffle partitions
# MAGIC b) the driver ran out of memory c) data skew on the shuffle key d) too many small files
# MAGIC
# MAGIC **Q3.** `MERGE` fails with "multiple source rows matched the same target row". Fix: a) deduplicate the source to one row per merge key
# MAGIC b) use a bigger cluster c) run VACUUM d) OPTIMIZE the target
# MAGIC
# MAGIC **Q4.** To keep exactly one row per key when deduplicating, use: a) `rank()` b) `dense_rank()` c) `row_number()` with a tie-breaker d) `ntile(1)`
# MAGIC
# MAGIC **Q5.** The main advantage of liquid clustering over Hive-style partitioning: a) you must rewrite the table to change keys b) keys can change without a full rewrite,
# MAGIC it handles high-cardinality columns, and it avoids small-file explosions c) it only works for streaming d) it disables data skipping
# MAGIC
# MAGIC **Q6.** Adaptive Query Execution can, at runtime: a) coalesce small shuffle partitions, switch join strategies, and split skewed partitions b) only cache tables
# MAGIC c) rewrite your Python UDFs d) turn Photon off
# MAGIC
# MAGIC **Q7.** A pandas UDF beats a regular Python UDF mainly because: a) it runs on the driver b) it transfers Arrow batches and runs vectorized code
# MAGIC c) it skips serialization entirely d) it runs inside the JVM
# MAGIC
# MAGIC **Q8.** The best way for a downstream job to process only rows that changed in a Delta table: a) a nightly full overwrite b) VACUUM
# MAGIC c) Change Data Feed or a streaming read of the table d) DESCRIBE HISTORY

# COMMAND ----------

answers = {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": ""}
for q, a in answers.items():
    card.mcq(q, a)

# COMMAND ----------

card.summary()
