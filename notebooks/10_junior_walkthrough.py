# Databricks notebook source
# MAGIC %md
# MAGIC # Level 1 - JUNIOR walkthrough
# MAGIC **Goal:** solid fundamentals. Solve easy problems cleanly, explain Big-O, write basic PySpark, and know the
# MAGIC vocabulary of data warehousing.
# MAGIC
# MAGIC **How to use this notebook:** read each section, run the code, and at every **CHECKPOINT** write down
# MAGIC your prediction *before* running the next cell. When you're done, take `11_junior_test`.
# MAGIC
# MAGIC | Part | Topics |
# MAGIC |---|---|
# MAGIC | A | How to attack any coding question |
# MAGIC | B | Hash maps & sets, strings, arrays, binary search, BFS |
# MAGIC | C | PySpark basics: lazy evaluation, transformations, joins, built-ins vs UDFs |
# MAGIC | D | Data warehousing vocabulary: facts, dimensions, grain, SCD1 vs SCD2, medallion |

# COMMAND ----------

import os, sys, time
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A - The 5-step method (use it on every problem, out loud)
# MAGIC 1. **Clarify**: input size, sorted?, duplicates?, negatives?, empty input?, what to return?
# MAGIC 2. **Brute force** + its Big-O. You always get *an* answer on the board.
# MAGIC 3. **Optimize**: which pattern removes the repeated work? (Usually: a hash map, sorting, or two pointers.)
# MAGIC 4. **Code + trace** one small example by hand.
# MAGIC 5. **State the complexity** and edge cases.
# MAGIC
# MAGIC ### Worked example: Two Sum
# MAGIC *Given `nums` and `target`, return indices of two numbers that add to target.*

# COMMAND ----------

def two_sum_brute(nums, target):
    # Check every pair: n*(n-1)/2 pairs -> O(n^2) time, O(1) space
    for i in range(len(nums)):
        for j in range(i + 1, len(nums)):
            if nums[i] + nums[j] == target:
                return [i, j]
    return []

def two_sum(nums, target):
    # For each x we need (target - x). A dict answers "have I seen it?" in O(1).
    seen = {}                      # value -> index
    for i, x in enumerate(nums):
        need = target - x
        if need in seen:
            return [seen[need], i]
        seen[x] = i                # store AFTER checking, so we never pair x with itself
    return []

print(two_sum([2, 7, 11, 15], 9))

# COMMAND ----------

# MAGIC %md
# MAGIC **Trace** of `two_sum([2, 7, 11, 15], 9)`:
# MAGIC
# MAGIC | i | x | need | seen before | action |
# MAGIC |---|---|---|---|---|
# MAGIC | 0 | 2 | 7 | {} | store 2->0 |
# MAGIC | 1 | 7 | 2 | {2:0} | **found** -> [0, 1] |
# MAGIC
# MAGIC **CHECKPOINT 1:** Is the dict version always faster? Predict the timings below for n = 4,000.

# COMMAND ----------

nums = list(range(4000))
for f in (two_sum_brute, two_sum):
    t = time.perf_counter(); f(nums, -1); print(f"{f.__name__:<14} {time.perf_counter() - t:.4f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** brute force does ~8 million comparisons; the dict does 4,000 lookups. For tiny inputs (n < 20) the
# MAGIC brute force can win because dict overhead dominates, which is worth saying in an interview. Big-O describes growth, not small cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B - Core patterns
# MAGIC ### B1. Sets for "have I seen this before?" (Contains Duplicate)

# COMMAND ----------

def contains_duplicate(nums):
    seen = set()
    for x in nums:
        if x in seen:          # O(1) average. The same check on a list is O(n), which makes the whole loop O(n^2)
            return True
        seen.add(x)
    return False

# One-liner version, same O(n): len(set(nums)) < len(nums)
print(contains_duplicate([1, 2, 3, 1]), contains_duplicate([1, 2, 3]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Counting with a hash map (Valid Anagram)
# MAGIC Two words are anagrams if they have the **same letter counts**. Sorting both is O(n log n); counting is O(n).

# COMMAND ----------

from collections import Counter

def is_anagram(a, b):
    if len(a) != len(b):       # cheap early exit
        return False
    return Counter(a) == Counter(b)

print(is_anagram("listen", "silent"), is_anagram("rat", "car"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 2:** What does `Counter("banana")` return? Predict it, then run the cell.

# COMMAND ----------

print(Counter("banana"))
print(Counter("banana").most_common(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. One pass with a running value (Best Time to Buy and Sell Stock)
# MAGIC *Max profit from buying once then selling later.* Brute force tries every buy/sell pair: O(n^2).
# MAGIC Insight: the best sale on day *i* buys at the **lowest price seen so far**, so track it as you go.

# COMMAND ----------

def max_profit(prices):
    lowest, best = float("inf"), 0
    for p in prices:
        lowest = min(lowest, p)          # best buy price so far
        best = max(best, p - lowest)     # best profit if we sell today
    return best

print(max_profit([7, 1, 5, 3, 6, 4]))   # buy at 1, sell at 6

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. Binary search: halve the search space (sorted input!)
# MAGIC **Invariant:** if the target exists, it's inside `nums[lo..hi]`. Each step removes half, so the cost is O(log n).
# MAGIC 1 billion items takes about 30 steps.
# MAGIC
# MAGIC The classic bugs are `while lo < hi` vs `lo <= hi`, and forgetting the `+1`/`-1` (which causes an infinite loop).

# COMMAND ----------

def binary_search(nums, target):
    lo, hi = 0, len(nums) - 1
    steps = 0
    while lo <= hi:                 # <= : a range with one element is still valid
        steps += 1
        mid = (lo + hi) // 2
        if nums[mid] == target:
            print(f"  found in {steps} steps")
            return mid
        if nums[mid] < target:
            lo = mid + 1            # target is right of mid
        else:
            hi = mid - 1            # target is left of mid
    print(f"  not found after {steps} steps")
    return -1

big = list(range(0, 2_000_000, 2))
print(binary_search(big, 1_337_336))
print(binary_search(big, 7))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 3:** What happens if the list is **not sorted**? Try `binary_search([5, 1, 4, 2], 1)` in your head.
# MAGIC
# MAGIC **Answer:** it can miss the value. mid=1 -> 1 == 1, found this time by luck. With target 5: mid=1 (1 < 5) -> lo=2 -> mid=2 (4 < 5) -> lo=3 -> mid=3 (2 < 5) -> not found, even though 5 is at index 0. *Always confirm the input is sorted.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### B5. BFS: explore level by level (graphs)
# MAGIC Graph = `dict[node] -> list of neighbors`. **BFS uses a queue**: it visits everything 1 hop away, then 2 hops, and so on.
# MAGIC That's why BFS finds the **fewest-hops** distance. Mark a node visited **when you enqueue it**, or it may be added twice.

# COMMAND ----------

from collections import deque

def bfs_levels(graph, start):
    dist = {start: 0}
    q = deque([start])           # deque.popleft() is O(1); list.pop(0) is O(n)
    while q:
        node = q.popleft()
        for nb in graph.get(node, []):
            if nb not in dist:   # dist doubles as the visited set
                dist[nb] = dist[node] + 1
                q.append(nb)
    return dist

# A tiny pipeline DAG: raw -> staging -> facts -> dashboard
g = {"raw": ["stg_orders", "stg_customers"], "stg_orders": ["fact_sales"], "stg_customers": ["dim_customer"],
     "dim_customer": ["fact_sales"], "fact_sales": ["dashboard"], "dashboard": []}
print(bfs_levels(g, "raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 4:** Why is `fact_sales` at distance 2 and not 3, even though one path to it is raw -> stg_customers -> dim_customer -> fact_sales?
# MAGIC
# MAGIC **Answer:** BFS reaches it first through the *shortest* path (raw -> stg_orders -> fact_sales). Once it's visited, the longer path is ignored.
# MAGIC Complexity: O(V + E), since every node and edge is touched once.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C - PySpark basics
# MAGIC ### C1. Transformations are lazy; actions run the job
# MAGIC - **Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) only *build a plan*. Nothing runs yet.
# MAGIC - **Actions** (`count`, `collect`, `show`, `display`, `write`) make Spark *optimize and run* the plan.
# MAGIC
# MAGIC Laziness lets Spark see the whole pipeline first and optimize it, for example by pushing filters down to the file scan.

# COMMAND ----------

from levels.data import junior
d = junior(spark)
orders, customers = d["orders"], d["customers"]

t = time.perf_counter()
plan_only = orders.filter("amount > 100").withColumn("big", F.lit(True))     # instant: nothing has run
print(f"transformations took {time.perf_counter() - t:.4f}s")
display(plan_only)                                                            # action -> the job runs now

# COMMAND ----------

# MAGIC %md ### C2. The everyday API: select / filter / withColumn / groupBy-agg

# COMMAND ----------

display(
    orders.filter(F.col("status") == "delivered")
          .withColumn("amount_with_tax", F.round(F.col("amount") * 1.08, 2))
          .groupBy("customer_id")
          .agg(F.count("*").alias("n_orders"), F.round(F.sum("amount"), 2).alias("revenue"))
          .orderBy(F.desc("revenue"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Join types. Memorize these.
# MAGIC | how | returns |
# MAGIC |---|---|
# MAGIC | `inner` | rows with a match on both sides |
# MAGIC | `left` | all left rows; right columns NULL when there's no match |
# MAGIC | `left_semi` | left rows that **have** a match (left columns only), "WHERE EXISTS" |
# MAGIC | `left_anti` | left rows that have **no** match, "WHERE NOT EXISTS" |
# MAGIC | `full` | everything from both sides |
# MAGIC
# MAGIC **CHECKPOINT 5:** customers 5 and 6 have no orders. Predict the row count for each join, then run.

# COMMAND ----------

for how in ["inner", "left", "left_semi", "left_anti"]:
    print(f"{how:<10}", customers.join(orders, "customer_id", how).count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** inner = 10 (one row per order). left = 12 (10 orders plus 2 customers with NULLs). left_semi = 4 (customers 1-4, **once each**,
# MAGIC because a semi join never duplicates rows). left_anti = 2 (customers 5 and 6).

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Built-in functions over Python UDFs, and why `collect()` is dangerous
# MAGIC - A **Python UDF** sends every row from the JVM to a Python process and back. It's slow and invisible to the optimizer.
# MAGIC - **Built-ins** (`F.upper`, `F.when`, `F.regexp_replace`, ...) run natively, can use Photon, and get optimized.
# MAGIC - `collect()` / `toPandas()` pulls **all rows to the driver** (one machine), which can run out of memory on big tables. Aggregate first.

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("string")
def shout(s):
    return s.upper()

display(customers.select("name", shout("name").alias("udf_upper"), F.upper("name").alias("builtin_upper")))

# Conditional logic without a UDF: when / otherwise
display(orders.select("order_id", "amount",
                      F.when(F.col("amount") >= 100, "high").otherwise("low").alias("tier")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D - Data warehousing vocabulary (your spike starts here)
# MAGIC | Term | Meaning | Example |
# MAGIC |---|---|---|
# MAGIC | **Fact table** | events/measurements: numbers + foreign keys | `fact_sales(order_id, date_sk, customer_sk, amount)` |
# MAGIC | **Dimension** | descriptive context | `dim_customer(customer_sk, name, region)` |
# MAGIC | **Grain** | what ONE fact row means. Decide this first! | "one row per order line" |
# MAGIC | **Star schema** | a fact in the middle, dimensions around it, one join hop each | |
# MAGIC | **Surrogate key** | a warehouse-generated ID (not the source system's) | `customer_sk` |
# MAGIC | **SCD Type 1** | overwrite the attribute, no history | fixing a typo in a name |
# MAGIC | **SCD Type 2** | new row per change, with validity dates | customer moved region |
# MAGIC | **Medallion** | Bronze (raw) -> Silver (clean, deduped) -> Gold (business-ready, star schemas) | |
# MAGIC
# MAGIC ### D1. SCD1 vs SCD2 on a tiny example

# COMMAND ----------

from datetime import date
from warehouse.scd import scd1_upsert, scd2_merge

dim = scd2_merge([], [{"customer_id": 1, "region": "NA"}], key="customer_id", tracked=["region"], as_of=date(2025, 1, 1))
change = [{"customer_id": 1, "region": "EMEA"}]

print("SCD1 (overwrite):")
for r in scd1_upsert([{"customer_id": 1, "region": "NA"}], change, key="customer_id"):
    print("  ", r)

print("SCD2 (history):")
for r in scd2_merge(dim, change, key="customer_id", tracked=["region"], as_of=date(2025, 6, 1)):
    print("  ", r)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 6:** A sale happened on 2025-03-15. With SCD2, which region does it report? With SCD1?
# MAGIC
# MAGIC **Answer:** SCD2 reports **NA**, because the sale joins to the row valid on 03-15. SCD1 reports **EMEA**, because history was overwritten,
# MAGIC so past sales are restated to today's region. Neither is "wrong". The business decides which question it is asking.
# MAGIC
# MAGIC ### D2. A star schema query, the shape you'll write most often

# COMMAND ----------

orders.createOrReplaceTempView("j_orders")
customers.createOrReplaceTempView("j_customers")
display(spark.sql("""
  SELECT c.region, count(*) AS orders, round(sum(o.amount), 2) AS revenue
  FROM j_orders o JOIN j_customers c ON o.customer_id = c.customer_id   -- fact joins dimension
  WHERE o.status = 'delivered'
  GROUP BY c.region
  ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready?
# MAGIC You should now be able to:
# MAGIC - [ ] explain why a dict/set makes lookups O(1) and when that beats sorting
# MAGIC - [ ] write binary search without off-by-one bugs, and BFS with a queue
# MAGIC - [ ] tell transformations from actions, and pick the right join type
# MAGIC - [ ] explain fact vs dimension, grain, SCD1 vs SCD2, and the medallion layers
# MAGIC
# MAGIC Next: **`11_junior_test`** (target score 85%).
