# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Complexity analysis & Python optimizations (driver-side, pure Python)
# MAGIC Goal: be able to *prove* a complexity claim, not just recite it.
# MAGIC Method: time at growing `n`, then fit the slope on a log-log scale. Slope ~1 = O(n), ~2 = O(n^2).

# COMMAND ----------

import os, sys, random, bisect
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from dsa import benchmark as bm, hash_maps, binary_search

# COMMAND ----------

# MAGIC %md ## 1. Slow vs fast pairs (see `dsa/python_optimizations.py` for the code)

# COMMAND ----------

display(spark.createDataFrame(bm.compare(n=3000)))

# COMMAND ----------

# MAGIC %md ## 2. Measure Big-O empirically

# COMMAND ----------

def two_sum_brute(nums, target):
    for i in range(len(nums)):
        for j in range(i + 1, len(nums)):
            if nums[i] + nums[j] == target:
                return [i, j]
    return []

rng = random.Random(0)
data = {n: [rng.randint(0, 10**9) for _ in range(n)] for n in [250, 500, 1000, 2000, 4000]}

experiments = {
    "two_sum brute  (expect ~2)": lambda n: two_sum_brute(data[n], -1),
    "two_sum hash   (expect ~1)": lambda n: hash_maps.two_sum(data[n], -1),
    "sorted()       (expect ~1.0-1.1, n log n)": lambda n: sorted(data[n]),
    "binary search  (expect ~0)": lambda n: [binary_search.lower_bound(sorted_cache[n], q) for q in range(200)],
}
sorted_cache = {n: sorted(v) for n, v in data.items()}

rows = []
for label, f in experiments.items():
    g = bm.growth(f, list(data))
    rows.append({"experiment": label, "estimated_exponent": round(bm.estimate_exponent(g), 2),
                 **{f"n={n}_ms": round(t * 1000, 3) for n, t in g}})
display(spark.createDataFrame(rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Talking points: complexity in a distributed system
# MAGIC - **Big-O per executor vs. total work.** A `groupBy` is O(n) work but the *shuffle* moves O(n) bytes over the network - network/disk IO usually dominates CPU.
# MAGIC - **Sort-merge join** = O(n log n + m log m) and spills when partitions exceed memory. **Broadcast hash join** = O(n + m) and no shuffle of the big side, but m must fit in each executor.
# MAGIC - **Skew breaks the parallelism assumption:** total work O(n)/P only holds if partitions are balanced. One hot key -> runtime = the largest partition.
# MAGIC - **`collect()` / `toPandas()`** turns a distributed O(n/P) into a single-machine O(n) *and* a memory cliff. Aggregate first, collect last.
# MAGIC - **Amortized analysis:** list append is O(1) amortized (doubling), dict ops O(1) average / O(n) worst on collisions - same reasoning as hash partitioning.
# MAGIC - **Space-time tradeoffs:** memoization (`lru_cache`) <-> Delta caching / materialized views / pre-aggregated gold tables.
