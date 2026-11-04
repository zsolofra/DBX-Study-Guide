# Databricks notebook source
# MAGIC %md
# MAGIC # Level 2 - MID walkthrough
# MAGIC **Goal:** recognize the *pattern* behind a medium problem, use window functions fluently, read a physical plan, and
# MAGIC implement SCD2 and point-in-time joins on Delta.
# MAGIC
# MAGIC | Part | Topics |
# MAGIC |---|---|
# MAGIC | A | Sliding window, prefix sums, binary search on the answer, topological sort, heaps, intervals |
# MAGIC | B | Window functions: `row_number` vs `rank`, `lag`, running totals |
# MAGIC | C | Reading plans: broadcast vs sort-merge, partial aggregation, skew detection, AQE, pandas UDFs |
# MAGIC | D | SCD2 with one Delta MERGE, point-in-time joins, clustering, Change Data Feed |
# MAGIC
# MAGIC At every **CHECKPOINT**, predict the result before you run the next cell. Then take `21_mid_test`.

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from collections import Counter, deque, defaultdict
import heapq
from pyspark.sql import Window, functions as F
from levels.data import mid
from spark_lab.utils import plan_string, plan_has

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
d = mid(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A - Patterns. The question to ask is "what work am I repeating?"
# MAGIC ### A1. Sliding window: longest substring without repeating characters
# MAGIC Brute force checks every substring, which is O(n^3) or O(n^2). A window `[left, right]` only moves forward, and each character enters
# MAGIC and leaves at most once, so it's **O(n)**. Store each character's last index so `left` can *jump* instead of crawl.

# COMMAND ----------

def longest_unique(s, verbose=False):
    last, left, best = {}, 0, 0
    for right, ch in enumerate(s):
        if last.get(ch, -1) >= left:        # repeat INSIDE the current window
            left = last[ch] + 1             # jump past the previous copy
        last[ch] = right
        best = max(best, right - left + 1)
        if verbose:
            print(f"right={right} ch={ch!r} window={s[left:right+1]!r:<8} best={best}")
    return best

longest_unique("abba", verbose=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 1:** Why does the code check `last.get(ch, -1) >= left` and not just `ch in last`?
# MAGIC
# MAGIC **Answer:** in `"abba"`, when we reach the final `a`, its earlier copy (index 0) is *outside* the window (left=2). Without the
# MAGIC `>= left` check, `left` would jump **backwards** to 1, giving the wrong answer 3 instead of 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Prefix sums + hash map: count subarrays summing to k
# MAGIC `sum(i..j) = prefix[j] - prefix[i-1]`. So for each `j`, we need to know how many earlier prefixes equal `prefix[j] - k`. A Counter tells us.
# MAGIC A sliding window **fails** here because negative numbers break the "grow/shrink" logic, which makes this a common follow-up question.

# COMMAND ----------

def subarray_sum_k(nums, k):
    seen, prefix, total = Counter({0: 1}), 0, 0     # {0:1}: the empty prefix, for subarrays starting at index 0
    for x in nums:
        prefix += x
        total += seen[prefix - k]
        seen[prefix] += 1
    return total

print(subarray_sum_k([1, -1, 0], 0))   # [1,-1], [0], [1,-1,0] -> 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Binary search on the ANSWER
# MAGIC Some problems ask for the *minimum X that works*, where "works" is **monotonic**: if X works, every bigger X works too.
# MAGIC Binary search over the possible values of X, and test each guess with an O(n) check.
# MAGIC
# MAGIC *Example: the smallest per-hour processing rate that finishes all `batches` within `h` hours.* (The test uses the same shape.)

# COMMAND ----------

def min_rate(batches, h):
    def hours_needed(rate):
        return sum(-(-b // rate) for b in batches)    # ceil division without floats
    lo, hi = 1, max(batches)                          # rate=max finishes each batch in 1 hour
    while lo < hi:
        mid = (lo + hi) // 2
        if hours_needed(mid) <= h:
            hi = mid          # mid works; maybe something smaller works too
        else:
            lo = mid + 1      # too slow
    return lo

print(min_rate([3, 6, 7, 11], 8))   # 4
# Talk track: "Same shape as finding the smallest cluster that meets the SLA. Total cost is O(n log(max))."

# COMMAND ----------

# MAGIC %md
# MAGIC ### A4. Topological sort (Kahn's algorithm): order work with dependencies
# MAGIC Count incoming edges (in-degree). Start with every node at in-degree 0, and as you "complete" a node, decrement its children.
# MAGIC If some nodes never reach 0, there's a **cycle**. This is exactly how a Databricks Job or pipeline schedules tasks.

# COMMAND ----------

def topo(tasks):            # tasks: node -> list of nodes that depend on it
    indeg = {t: 0 for t in tasks}
    for deps in tasks.values():
        for d_ in deps:
            indeg[d_] += 1
    q = deque(t for t, n in indeg.items() if n == 0)
    order = []
    while q:
        t = q.popleft()
        order.append(t)
        for d_ in tasks[t]:
            indeg[d_] -= 1
            if indeg[d_] == 0:
                q.append(d_)
    return order if len(order) == len(tasks) else "CYCLE"

print(topo({"bronze": ["silver"], "silver": ["dim", "fact"], "dim": ["fact"], "fact": ["gold"], "gold": []}))
print(topo({"a": ["b"], "b": ["a"]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A5. Heaps for top-k, and sorting with a tie-breaker
# MAGIC A full sort is O(n log n). A heap of size k is O(n log k). Sorting by `(-count, word)` gives "highest count first, then
# MAGIC alphabetical", a trick you'll use often.

# COMMAND ----------

words = ["spark", "delta", "spark", "photon", "delta", "spark", "unity"]
counts = Counter(words)
print(heapq.nsmallest(2, counts.items(), key=lambda wc: (-wc[1], wc[0])))
print(sorted(counts, key=lambda w: (-counts[w], w)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A6. Intervals: sort, then sweep
# MAGIC Sort by start. Each new interval either overlaps the last merged one (extend it) or starts a new one. O(n log n).
# MAGIC In the warehouse, this is how you check that SCD2 validity windows never overlap.

# COMMAND ----------

def merge_intervals(iv):
    out = []
    for s, e in sorted(iv):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out

print(merge_intervals([[8, 10], [1, 3], [2, 6], [15, 18]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B - Window functions. The most-used tool in analytics SQL.
# MAGIC A window = `PARTITION BY` (groups) + `ORDER BY` (sequence) + an optional **frame** (which rows count).
# MAGIC Unlike `groupBy`, a window function **keeps every row**.

# COMMAND ----------

raw = d["raw_orders"]
display(raw.orderBy("order_id", "ingested_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. `row_number` vs `rank` vs `dense_rank`
# MAGIC **CHECKPOINT 2:** customer 20 has two orders on the **same date** (2025-02-11). Predict all three columns for those rows.

# COMMAND ----------

orders = d["orders"]
w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))
display(orders.select("customer_id", "order_id", "order_date",
                      F.row_number().over(w).alias("row_number"),
                      F.rank().over(w).alias("rank"),
                      F.dense_rank().over(w).alias("dense_rank")).orderBy("customer_id", "row_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** `rank` gives both rows **1**, so the next row gets 3. `dense_rank` gives 1, 1, 2. `row_number` gives 1 and 2, but *which* tied row gets 1
# MAGIC is arbitrary. For **dedupe**, use `row_number` (exactly one row per key) and add a **tie-breaker** column (`order_id`) so it's deterministic.
# MAGIC With `rank`, ties would keep duplicates.

# COMMAND ----------

# MAGIC %md ### B2. Dedupe: latest event per key (bronze -> silver)

# COMMAND ----------

latest = Window.partitionBy("order_id").orderBy(F.desc("ingested_at"))
display(raw.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn").orderBy("order_id"))
# Note order 3: 'returned' was ingested last, even though 'delivered' has a later event in the business flow.
# Interview follow-up: "order by ingestion time or by event time?" Know which one the business means.

# COMMAND ----------

# MAGIC %md ### B3. `lag` / `lead` and running totals (frames)

# COMMAND ----------

monthly = orders.groupBy(F.date_format("order_date", "yyyy-MM").alias("month")).agg(F.sum("amount").alias("revenue"))
w_m = Window.orderBy("month")
display(monthly.select(
    "month", "revenue",
    F.lag("revenue").over(w_m).alias("prev_month"),
    F.round((F.col("revenue") / F.lag("revenue").over(w_m) - 1) * 100, 2).alias("mom_pct"),
    F.sum("revenue").over(w_m.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("running_total"),
).orderBy("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 3:** `Window.orderBy("month")` has no `partitionBy`. What does Spark warn about, and when does it matter?
# MAGIC
# MAGIC **Answer:** "No Partition Defined for Window operation", meaning all rows move to **one partition**. That's fine for 12 monthly rows and
# MAGIC terrible for 1 billion raw rows. **Aggregate first, then window**, exactly as the cell above does.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C - Reading physical plans
# MAGIC Read the plan **bottom-up**: scan -> filter -> (partial agg) -> **Exchange** (= shuffle, the expensive part) -> join/agg -> output.

# COMMAND ----------

big = spark.range(2_000_000).select((F.col("id") % 50_000).alias("customer_id"), (F.rand(1) * 100).alias("amount"))
small = spark.range(50_000).select(F.col("id").alias("customer_id"), (F.col("id") % 4).alias("region_id"))

q = big.join(small, "customer_id").groupBy("region_id").agg(F.sum("amount"))
print(plan_string(q))

# COMMAND ----------

# MAGIC %md
# MAGIC **What to point at:**
# MAGIC - `BroadcastExchange` + `BroadcastHashJoin`: the small side is copied to every executor, so the big side is **never shuffled**.
# MAGIC - `HashAggregate` appears **twice**: a partial aggregate before the `Exchange` (each partition pre-sums its data) and a final one after it.
# MAGIC   That's why `groupBy().sum()` shuffles at most one row per group per partition instead of every row.
# MAGIC - `AdaptiveSparkPlan`: AQE may change the plan at runtime (`isFinalPlan=true` after it runs).
# MAGIC
# MAGIC **CHECKPOINT 4:** Force a sort-merge join with `small.hint("merge")`. How many `Exchange` nodes do you expect?

# COMMAND ----------

q_smj = big.join(small.hint("merge"), "customer_id").groupBy("region_id").agg(F.sum("amount"))
p = plan_string(q_smj)
print(plan_has(q_smj, "SortMergeJoin", "BroadcastHashJoin"))
print("Exchange nodes:", p.count("Exchange"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** about 3: one shuffle per join side (hash-partition by `customer_id`) plus one for the aggregation. The broadcast version has no shuffle of the big side.
# MAGIC Broadcasting only works while the small side fits in memory (default threshold 10 MB; AQE also converts joins at runtime using real sizes).

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Detecting skew without the Spark UI
# MAGIC In the UI, skew looks like one task taking 10-100x the median. In code, count rows per key and per partition.

# COMMAND ----------

skewed = big.withColumn("customer_id", F.when(F.rand(7) < 0.5, F.lit(0)).otherwise(F.col("customer_id")))
display(skewed.groupBy("customer_id").count().orderBy(F.desc("count")).limit(5))
display(skewed.repartition(16, "customer_id").groupBy(F.spark_partition_id().alias("pid")).count().orderBy(F.desc("count")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fix ladder, in order:**
# MAGIC 1. Is the hot key junk (NULL, -1, "unknown")? Filter it out or process it separately.
# MAGIC 2. Broadcast the other side.
# MAGIC 3. **AQE skew join** (on by default on Databricks) splits oversized partitions.
# MAGIC 4. Salting, for when the others aren't enough (Senior level covers this).
# MAGIC
# MAGIC ### C3. pandas UDF vs Python UDF vs built-in
# MAGIC A pandas UDF moves **Arrow batches** and runs vectorized pandas/numpy code, which is far faster than a row-at-a-time Python UDF. It's still slower than
# MAGIC a built-in, so only use it when you need Python libraries. Demos: `03_pyspark_optimization_lab` section 5, and `05_spot_the_antipattern`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D - Warehousing on Delta
# MAGIC ### D1. SCD2 in ONE MERGE (the "staged updates" trick)
# MAGIC A `MERGE` can either UPDATE or INSERT for each source row, but a change needs **both**: close the old version *and* insert the new one.
# MAGIC The trick is to send each changed row **twice**:
# MAGIC 1. with `merge_key = customer_id`, which matches the current row, so we UPDATE it closed
# MAGIC 2. with `merge_key = NULL`, which never matches, so we INSERT the new version

# COMMAND ----------

from warehouse.delta_scd import scd2_merge_sql

spark.sql("""CREATE OR REPLACE TABLE lvl_dim_customer (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY, customer_id INT NOT NULL, region STRING, segment STRING,
  row_hash STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN)""")
sql = scd2_merge_sql("lvl_dim_customer", "lvl_changes", "customer_id", ["region", "segment"])
print(sql)

# COMMAND ----------

def load(rows, as_of):
    spark.createDataFrame(rows, "customer_id int, region string, segment string") \
         .withColumn("effective_date", F.lit(as_of).cast("date")).createOrReplaceTempView("lvl_changes")
    return spark.sql(sql)

display(load([(1, "NA", "smb"), (2, "EMEA", "consumer")], "2025-01-01"))                 # initial load
display(load([(1, "APAC", "smb"), (2, "EMEA", "consumer"), (3, "NA", "enterprise")], "2025-06-01"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 5:** After the second load, how many rows does `lvl_dim_customer` have, and which are current?

# COMMAND ----------

display(spark.sql("SELECT * FROM lvl_dim_customer ORDER BY customer_id, valid_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** 4 rows. Customer 1 has two versions (NA closed on 06-01, APAC current). Customer 2 is unchanged (its hash matched, so nothing happened).
# MAGIC Customer 3 is new. **Re-running the same batch changes nothing**, which makes the load idempotent. Interviewers ask about this.
# MAGIC
# MAGIC Gotchas: the source must have **one row per key** (otherwise MERGE fails with "multiple source rows matched"), and the
# MAGIC hash makes NULL-safe change detection a single comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Point-in-time join: attach the dimension version valid on the fact's date
# MAGIC The condition is a **half-open interval**, `valid_from <= sale_date < valid_to`, with `valid_to IS NULL` meaning "still current".
# MAGIC Half-open means a date exactly on a boundary matches **exactly one** version.

# COMMAND ----------

sales, dim_hist = d["sales"], d["dim_hist"]
cond = ((sales.customer_id == dim_hist.customer_id) & (sales.sale_date >= dim_hist.valid_from)
        & (dim_hist.valid_to.isNull() | (sales.sale_date < dim_hist.valid_to)))
display(sales.join(dim_hist, cond, "left").select(sales["*"], dim_hist.region).orderBy("sale_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 6:** Sale 2 is on 2025-02-01, exactly when customer 10 switched from NA to EMEA. Which region, and why not both?
# MAGIC
# MAGIC **Answer:** **EMEA**. NA's window is `[2024-01-01, 2025-02-01)`, which excludes 02-01. If you used `<=` on both ends, the sale would match both
# MAGIC versions and the fact row would be **duplicated**, a classic fan-out bug that inflates revenue. Sale 7 has no customer, so its region is NULL: the left join keeps it.
# MAGIC In production you'd map it to an "unknown member" row instead (sk = -1).

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Layout and incremental processing: what to say
# MAGIC - **Liquid clustering** (`CLUSTER BY (date_sk, customer_sk)`): groups related rows into the same files, so per-file min/max stats let queries **skip files**.
# MAGIC   The keys can change without a rewrite, so prefer it over Hive partitioning for most tables.
# MAGIC - **OPTIMIZE** compacts small files. **Predictive optimization** runs OPTIMIZE, VACUUM and ANALYZE automatically.
# MAGIC - **Change Data Feed** (`delta.enableChangeDataFeed = true`, read with `table_changes()` or `readChangeFeed`): downstream jobs process only
# MAGIC   rows that changed instead of re-reading the whole table.

# COMMAND ----------

spark.sql("ALTER TABLE lvl_dim_customer SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
v = spark.sql("DESCRIBE HISTORY lvl_dim_customer").agg(F.max("version")).first()[0]
load([(2, "LATAM", "consumer")], "2025-09-01")
display(spark.sql(f"SELECT customer_id, region, valid_to, is_current, _change_type, _commit_version FROM table_changes('lvl_dim_customer', {v + 1}) ORDER BY customer_id, _change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready?
# MAGIC - [ ] Name the pattern within 2 minutes for: longest unique substring, subarray sum = k, min rate within h hours, course order, top-k, merge intervals
# MAGIC - [ ] Explain why `row_number` (with a tie-breaker) and not `rank` for dedupe
# MAGIC - [ ] Read a plan: find the Exchanges, broadcast vs sort-merge, partial aggregation
# MAGIC - [ ] Explain the SCD2 staged-updates MERGE and the half-open point-in-time join
# MAGIC
# MAGIC Next: **`21_mid_test`** (target 85%).
