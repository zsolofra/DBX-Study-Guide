# Databricks notebook source
# MAGIC %md
# MAGIC # Level 2: Mid Patterns & Performance
# MAGIC **Course:** DBX Study Guide | **Duration:** ~2.5 hours | **Prerequisite:** Level 1 | **Next:** `21_mid_test`
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this class you will be able to:
# MAGIC 1. Recognize and apply **sliding window**, **prefix sum**, **binary search on the answer**, **topological sort**, **heap** and **interval** patterns
# MAGIC 2. Use **window functions** (`row_number`, `rank`, `lag`, frames) for dedupe, ranking and running totals
# MAGIC 3. **Read a physical plan**: find shuffles, recognize broadcast vs sort-merge joins, and spot partial aggregation
# MAGIC 4. **Detect skew** and explain what AQE does about it
# MAGIC 5. Implement **SCD Type 2 with a single Delta MERGE** and a **point-in-time join**
# MAGIC
# MAGIC | Marker | What to do |
# MAGIC |---|---|
# MAGIC | **CONCEPT** | read |
# MAGIC | **DEMO** | run it, then read the **Breakdown** |
# MAGIC | **KNOWLEDGE CHECK** | predict before you run |
# MAGIC | **YOUR TURN** | code it; the cell checks itself (`lesson.hint("id")`, `lesson.solution("id")`) |

# COMMAND ----------

# MAGIC %md ## Classroom setup

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

import heapq
from collections import Counter, defaultdict, deque
from spark_lab.utils import plan_string, plan_has

d = lesson.data("mid")
orders, raw_orders, sales, dim_hist = d["orders"], d["raw_orders"], d["sales"], d["dim_hist"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 1: Sliding window

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT
# MAGIC For problems about **contiguous** subarrays or substrings, brute force re-examines overlapping ranges again and again.
# MAGIC A **window** `[left, right]` reuses the previous answer: add what enters, remove what leaves.
# MAGIC - **Fixed size k**: slide one step at a time, giving O(n).
# MAGIC - **Variable size**: grow `right`, and shrink `left` when the window breaks the rule. Each index enters and leaves once, so it's O(n).
# MAGIC
# MAGIC **Spark connection:** `ROWS BETWEEN k-1 PRECEDING AND CURRENT ROW` is a sliding window in SQL.

# COMMAND ----------

# MAGIC %md ### DEMO: fixed window, a moving 3-day sum

# COMMAND ----------

daily = [10, 20, 30, 5, 5, 40]
k = 3
window = sum(daily[:k])
print(f"days 0-2: {window}")
for i in range(k, len(daily)):
    window += daily[i] - daily[i - k]
    print(f"days {i-k+1}-{i}: {window}   (+{daily[i]} entering, -{daily[i-k]} leaving)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** each step is O(1) (one add and one subtract) instead of re-summing k numbers. That makes it O(n) total instead of O(n*k).

# COMMAND ----------

# MAGIC %md ### DEMO: variable window, longest substring without repeats

# COMMAND ----------

def longest_unique(s):
    last, left, best = {}, 0, 0
    for right, ch in enumerate(s):
        if last.get(ch, -1) >= left:          # repeat INSIDE the current window
            left = last[ch] + 1               # jump left past the earlier copy
        last[ch] = right
        best = max(best, right - left + 1)
        print(f"right={right} ch={ch!r}  window={s[left:right+1]!r:<8} best={best}")
    return best

longest_unique("pwwkew")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** at `right=2` the second `w` repeats inside the window, so `left` jumps to 2. The window never moves backwards, which is what makes it O(n).

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Why check `last.get(ch, -1) >= left` and not just `ch in last`? Hint: try `"abba"`.

# COMMAND ----------

longest_unique("abba")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** at the final `a`, the earlier `a` (index 0) is already **outside** the window (left = 2). Using `ch in last` would move `left` *back* to 1
# MAGIC and report 3. Characters you've already passed must not pull the window backwards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-1): Max sum of any window of size k
# MAGIC `max_sum_window([1,4,2,10,23,3,1,0,20], 4)` returns `39`. Assume `len(nums) >= k`. Target: O(n).

# COMMAND ----------

def max_sum_window(nums, k):
    # TODO
    raise NotImplementedError

lesson.check("M-1", max_sum_window)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 2: Prefix sums + hash maps

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT
# MAGIC `prefix[i]` = the sum of the first i elements. Then **any range sum is a subtraction**: `sum(i..j) = prefix[j+1] - prefix[i]`, which is O(1).
# MAGIC Combined with a hash map of "how many times have I seen each prefix value?", you can count subarrays with a target sum in **O(n)**,
# MAGIC **even with negative numbers**, where a sliding window breaks.

# COMMAND ----------

nums = [3, 4, -7, 1, 3, 3, 1, -4]
prefix = [0]
for x in nums:
    prefix.append(prefix[-1] + x)
print("nums  :", nums)
print("prefix:", prefix)
print("sum of nums[3..5] =", prefix[6] - prefix[3], "=", sum(nums[3:6]))

# COMMAND ----------

# MAGIC %md ### DEMO: count subarrays that sum to k

# COMMAND ----------

def subarray_sum_k(nums, k):
    seen = Counter({0: 1})          # the empty prefix, for subarrays that start at index 0
    running = total = 0
    for x in nums:
        running += x
        found = seen[running - k]   # earlier prefixes P with running - P == k
        total += found
        seen[running] += 1
        print(f"x={x:>2} prefix={running:>2} need={running-k:>2} found={found} total={total}")
    return total

subarray_sum_k([1, -1, 0], 0)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** the answer is 3: `[1,-1]`, `[0]` and `[1,-1,0]`. Each time the same prefix value appears again, the elements in between sum to 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Why doesn't a sliding window work for "subarray sum = k" when there are negatives?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** a sliding window relies on *growing the window increasing the sum and shrinking it decreasing the sum*. Negative numbers break that,
# MAGIC so you can't decide which side to move. Prefix sums don't depend on that property.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-2): Pivot index
# MAGIC Return the leftmost index where the sum of the elements to the **left** equals the sum to the **right**, or `-1`. `[1,7,3,6,5,6]` returns `3`. Target: O(n).

# COMMAND ----------

def pivot_index(nums):
    # TODO
    raise NotImplementedError

lesson.check("M-2", pivot_index)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 3: Binary search on the answer

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT
# MAGIC Some problems ask for *"the minimum X such that something works."* If "works" is **monotonic** (once X works, every bigger X works), the
# MAGIC answers look like this:
# MAGIC ```
# MAGIC X:      1   2   3   4   5   6   7 ...
# MAGIC works?  no  no  no  YES YES YES YES     <- binary search for the first YES
# MAGIC ```
# MAGIC Cost: O(log(range)) guesses times the cost of checking one guess.
# MAGIC
# MAGIC **Spark connection:** "smallest cluster that meets the SLA" or "fewest partitions under 200 MB each" have the same shape.

# COMMAND ----------

def hours_needed(batches, rate):
    return sum(-(-b // rate) for b in batches)      # ceil(b / rate) without floats

def min_rate(batches, h):
    lo, hi = 1, max(batches)
    while lo < hi:
        mid = (lo + hi) // 2
        ok = hours_needed(batches, mid) <= h
        print(f"lo={lo:<3} hi={hi:<3} try rate={mid:<3} hours={hours_needed(batches, mid):<3} {'works -> go lower' if ok else 'too slow -> go higher'}")
        if ok:
            hi = mid
        else:
            lo = mid + 1
    return lo

print("answer:", min_rate([3, 6, 7, 11], h=8))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** we never tried rates one by one. When a rate works, `hi = mid` keeps it as a candidate. When it's too slow, `lo = mid + 1` discards it.
# MAGIC The loop ends when `lo == hi`, which is the smallest rate that works.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-3): Smallest divisor
# MAGIC Find the smallest positive integer `d` such that `sum(ceil(n / d) for n in nums) <= threshold`. `([1,2,5,9], 6)` returns `5`.

# COMMAND ----------

def smallest_divisor(nums, threshold):
    # TODO
    raise NotImplementedError

lesson.check("M-3", smallest_divisor)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 4: Topological sort, ordering work with dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Kahn's algorithm
# MAGIC 1. Count each node's **in-degree** (how many things it depends on).
# MAGIC 2. Put every node with in-degree 0 in a queue. They can run now.
# MAGIC 3. Pop a node, add it to the order, and decrement its children's in-degrees. Any child that reaches 0 joins the queue.
# MAGIC 4. If some nodes never reach 0, there's a **cycle**.
# MAGIC
# MAGIC This is how **Databricks Jobs** and **Lakeflow pipelines** decide task order.

# COMMAND ----------

def topo(tasks, trace=True):
    indeg = {t: 0 for t in tasks}
    for children in tasks.values():
        for c in children:
            indeg[c] += 1
    q = deque(t for t, n in indeg.items() if n == 0)
    order = []
    while q:
        t = q.popleft()
        order.append(t)
        for c in tasks[t]:
            indeg[c] -= 1
            if indeg[c] == 0:
                q.append(c)
        if trace:
            print(f"ran {t:<8} ready queue={list(q)}")
    return order if len(order) == len(tasks) else None

pipeline = {"bronze": ["silver"], "silver": ["dim", "fact"], "dim": ["fact"], "fact": ["gold"], "gold": []}
topo(pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** `fact` waited until **both** `silver` and `dim` were done, because its in-degree went 2, then 1, then 0. Tasks waiting in the queue together
# MAGIC could run **in parallel**, which is exactly what a job scheduler does.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC What does `topo({"a": ["b"], "b": ["c"], "c": ["a"]})` return?

# COMMAND ----------

print(topo({"a": ["b"], "b": ["c"], "c": ["a"]}))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** `None`. No node ever has in-degree 0, so nothing can start. That's a cycle, and a job definition like this would be rejected.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-4): Can all courses be finished?
# MAGIC Courses are `0..n-1`, and `[course, prereq]` means prereq comes first. Return `True` if an order exists.

# COMMAND ----------

def can_finish(n, prereqs):
    # TODO
    raise NotImplementedError

lesson.check("M-4", can_finish)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 5: Heaps and intervals

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Heaps
# MAGIC `heapq` is a **min-heap**: `heap[0]` is always the smallest. Push and pop are O(log n).
# MAGIC To keep the **k largest** items, keep a heap of size k and pop the smallest whenever it grows past k. That's O(n log k), better than sorting everything.

# COMMAND ----------

heap = []
for x in [5, 1, 8, 3, 9, 2]:
    heapq.heappush(heap, x)
    if len(heap) > 3:
        heapq.heappop(heap)
    print(f"pushed {x}: heap={sorted(heap)}  (root = {heap[0]})")
print("3rd largest =", heap[0])

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** the heap holds the 3 biggest values seen so far, and its root is the *smallest of those*, which is the 3rd largest overall.
# MAGIC Spark's `orderBy().limit(k)` does the same thing per partition (TakeOrderedAndProject), then merges the results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Intervals, sort then sweep
# MAGIC Sort by start. Each interval either overlaps the last merged one (extend its end) or starts a new group.

# COMMAND ----------

def merge_intervals(iv):
    out = []
    for s, e in sorted(iv):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
            print(f"[{s},{e}] overlaps, extend to {out[-1]}")
        else:
            out.append([s, e])
            print(f"[{s},{e}] starts a new interval")
    return out

merge_intervals([[8, 10], [1, 3], [2, 6], [15, 18]])

# COMMAND ----------

# MAGIC %md
# MAGIC **Warehouse connection:** SCD2 validity windows for one customer must **never overlap**. Checking that is this algorithm.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-5): k-th largest element
# MAGIC `kth_largest([3,2,1,5,6,4], 2)` returns `5`. Use a heap of size k (no full sort).

# COMMAND ----------

def kth_largest(nums, k):
    # TODO
    raise NotImplementedError

lesson.check("M-5", kth_largest)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 6: Window functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: groupBy vs window
# MAGIC - `groupBy` **collapses** rows: one row per group.
# MAGIC - A **window function** **keeps every row** and adds a value computed over a set of related rows.
# MAGIC
# MAGIC A window has three parts:
# MAGIC ```
# MAGIC Window.partitionBy("customer_id")     <- which rows belong together
# MAGIC       .orderBy("order_date")          <- their sequence
# MAGIC       .rowsBetween(start, end)        <- (optional) the FRAME: which of those rows to include
# MAGIC ```

# COMMAND ----------

display(orders.orderBy("customer_id", "order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Customer 20 has **two orders on 2025-02-11** (orders 6 and 7). Ordering by `order_date` descending, predict `row_number`, `rank` and `dense_rank` for customer 20's three rows.

# COMMAND ----------

w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))
display(orders.select("customer_id", "order_id", "order_date",
                      F.row_number().over(w).alias("row_number"),
                      F.rank().over(w).alias("rank"),
                      F.dense_rank().over(w).alias("dense_rank"))
              .filter("customer_id = 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer and breakdown**
# MAGIC | | 2/11 (tie) | 2/11 (tie) | 1/11 |
# MAGIC |---|---|---|---|
# MAGIC | `row_number` | 1 | 2 | 3 |
# MAGIC | `rank` | 1 | 1 | **3** (skips 2) |
# MAGIC | `dense_rank` | 1 | 1 | 2 |
# MAGIC
# MAGIC With `row_number`, *which* tied row gets 1 is arbitrary unless you add a tie-breaker, e.g. `.orderBy(F.desc("order_date"), F.desc("order_id"))`.

# COMMAND ----------

# MAGIC %md ### DEMO: Dedupe to the latest record per key (the bronze to silver step)

# COMMAND ----------

display(raw_orders.orderBy("order_id", "ingested_at"))

# COMMAND ----------

latest = Window.partitionBy("order_id").orderBy(F.desc("ingested_at"))
deduped = raw_orders.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")
display(deduped.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** one shuffle (partition by `order_id`) and a local sort, with no self-join. Use `row_number` here, **not** `rank`: with `rank`, ties would keep duplicates.
# MAGIC Interview follow-up: *"latest by ingestion time or by event time?"* Order 3 shows the difference.

# COMMAND ----------

# MAGIC %md ### DEMO: `lag` for period-over-period, and frames for running totals

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
# MAGIC **Breakdown:** `lag` looks one row back, and the first month has no previous row, so it's NULL. The frame `unboundedPreceding .. currentRow` sums every row so far.
# MAGIC
# MAGIC > **Warning:** `Window.orderBy` **without `partitionBy`** moves all rows into one partition. That's fine here because we **aggregated to 3 monthly rows first**.
# MAGIC > On a billion raw rows it would be a single-task bottleneck.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-6): Each customer's first order
# MAGIC Columns **`customer_id`** and **`order_id`**: the earliest `order_date` per customer, breaking ties by the **lowest** `order_id`.

# COMMAND ----------

m6 = None   # TODO

lesson.check("M-6", m6)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 7: Reading physical plans

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: What to look for (read bottom-up)
# MAGIC | Operator | Meaning | Cost |
# MAGIC |---|---|---|
# MAGIC | `Scan` / `FileScan` with `PushedFilters` | reading data, with filters applied during the read | IO |
# MAGIC | **`Exchange`** | a **shuffle**: data is redistributed over the network | the expensive one |
# MAGIC | `BroadcastExchange` + `BroadcastHashJoin` | the small side is copied to every executor | no shuffle of the big side |
# MAGIC | `SortMergeJoin` | both sides shuffled and sorted by key | 2 shuffles + sorts |
# MAGIC | `HashAggregate` (twice) | partial aggregate before the shuffle, final after | shuffles one row per group per partition |
# MAGIC | `AdaptiveSparkPlan` | AQE may change the plan at runtime | |

# COMMAND ----------

big = spark.range(2_000_000).select((F.col("id") % 50_000).alias("customer_id"), (F.rand(1) * 100).alias("amount"))
small = spark.range(50_000).select(F.col("id").alias("customer_id"), (F.col("id") % 4).alias("region_id"))
q = big.join(small, "customer_id").groupBy("region_id").agg(F.sum("amount").alias("revenue"))
print(plan_string(q))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown, bottom-up:**
# MAGIC 1. Two `Range` scans (our generated data).
# MAGIC 2. `BroadcastExchange` on the 50k-row side, then **`BroadcastHashJoin`**: the 2M-row side is joined in place with no shuffle.
# MAGIC 3. `HashAggregate (partial)`: each partition pre-sums per region.
# MAGIC 4. `Exchange hashpartitioning(region_id)`: only about 4 rows per partition are shuffled.
# MAGIC 5. `HashAggregate (final)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Force a sort-merge join with `small.hint("merge")`. How many `Exchange` operators will the plan have?

# COMMAND ----------

q_smj = big.join(small.hint("merge"), "customer_id").groupBy("region_id").agg(F.sum("amount"))
print(plan_has(q_smj, "SortMergeJoin", "BroadcastHashJoin"))
print("Exchange operators:", plan_string(q_smj).count("Exchange"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** about **3**: both join sides are hash-partitioned by `customer_id`, plus one for the aggregation. The broadcast plan only shuffled the tiny partial aggregate.
# MAGIC Broadcast works while the small side fits in memory (the default threshold is 10 MB, and AQE also converts at runtime using actual sizes).

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEMO: Detecting skew
# MAGIC In the Spark UI, skew looks like **one task running 10-100x longer than the median**. In code, count rows per key and per partition:

# COMMAND ----------

skewed = big.withColumn("customer_id", F.when(F.rand(7) < 0.5, F.lit(0)).otherwise(F.col("customer_id")))
display(skewed.groupBy("customer_id").count().orderBy(F.desc("count")).limit(5))

# COMMAND ----------

display(skewed.repartition(16, "customer_id")
              .groupBy(F.spark_partition_id().alias("partition")).count()
              .orderBy(F.desc("count")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** half of all rows have `customer_id = 0`, and `hash(0) % 16` sends them all to **one partition**. The job can't finish until that one task does.
# MAGIC
# MAGIC **What AQE does at runtime:** it coalesces tiny shuffle partitions, switches sort-merge to broadcast when one side turns out small, and **splits skewed partitions**
# MAGIC in sort-merge joins. On Databricks it's on by default. (Senior level covers salting.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-7): Make it a broadcast join
# MAGIC Join `orders` to the **current** rows of `dim_hist` (`valid_to IS NULL`) on `customer_id`. Output columns **`order_id`** and **`region`**.
# MAGIC The checker verifies the rows **and** that the plan contains `BroadcastHashJoin`.

# COMMAND ----------

m7 = None   # TODO

lesson.check("M-7", m7)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 8: SCD Type 2 on Delta (your spike)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Why SCD2 needs a trick in MERGE
# MAGIC For each source row, a `MERGE` either **updates** a matched row or **inserts** an unmatched one. A changed customer needs **both**:
# MAGIC close the old version *and* insert a new one. The trick is to stage each **changed** row **twice**:
# MAGIC
# MAGIC | Copy | `merge_key` | Matches the current row? | Result |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `customer_id` | yes | `WHEN MATCHED`: close it (`valid_to`, `is_current=false`) |
# MAGIC | 2 | `NULL` | never | `WHEN NOT MATCHED`: insert the new version |
# MAGIC
# MAGIC Brand-new customers only need copy 1, which doesn't match and gets inserted. Unchanged customers match, but their hash is equal, so nothing happens.

# COMMAND ----------

# MAGIC %md ### Step 1: Create the dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lvl_dim_customer (
# MAGIC   customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,   -- surrogate key, assigned by Delta
# MAGIC   customer_id INT NOT NULL,                          -- business key
# MAGIC   region STRING, segment STRING,
# MAGIC   row_hash STRING,                                   -- one column to compare instead of N
# MAGIC   valid_from DATE, valid_to DATE, is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %md ### Step 2: Initial load (the same MERGE, against an empty table)

# COMMAND ----------

from warehouse.delta_scd import scd2_merge_sql, row_hash_expr

MERGE_SQL = scd2_merge_sql("lvl_dim_customer", "lvl_changes", "customer_id", ["region", "segment"])

def stage_changes(rows, as_of):
    (spark.createDataFrame(rows, "customer_id int, region string, segment string")
          .withColumn("effective_date", F.lit(as_of).cast("date"))
          .createOrReplaceTempView("lvl_changes"))

stage_changes([(1, "NA", "smb"), (2, "EMEA", "consumer")], "2025-01-01")
display(spark.sql(MERGE_SQL))

# COMMAND ----------

display(spark.table("lvl_dim_customer"))

# COMMAND ----------

# MAGIC %md ### Step 3: A change batch. First, look at the staged source the MERGE will use.

# COMMAND ----------

stage_changes([(1, "APAC", "smb"), (2, "EMEA", "consumer"), (3, "NA", "enterprise")], "2025-06-01")

h = row_hash_expr(["s.region", "s.segment"])
display(spark.sql(f"""
  SELECT s.customer_id AS merge_key, s.customer_id, s.region, s.segment, 'copy 1' AS copy FROM lvl_changes s
  UNION ALL
  SELECT NULL AS merge_key, s.customer_id, s.region, s.segment, 'copy 2' AS copy FROM lvl_changes s
  JOIN lvl_dim_customer t ON t.customer_id = s.customer_id AND t.is_current
  WHERE t.row_hash <> {h}
  ORDER BY customer_id, copy
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown of the staged rows**
# MAGIC - Customer **1** (NA to APAC) appears **twice**: copy 1 closes the NA row, and copy 2 (`merge_key` NULL) inserts APAC.
# MAGIC - Customer **2** appears once. It will match, but the hash is unchanged, so **no-op**.
# MAGIC - Customer **3** appears once and matches nothing, so **insert**.

# COMMAND ----------

# MAGIC %md ### Step 4: Run the MERGE

# COMMAND ----------

display(spark.sql(MERGE_SQL))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC How many rows are in `lvl_dim_customer` now, and which are current? Then: what happens if you **run the same MERGE again**?

# COMMAND ----------

display(spark.sql("SELECT customer_id, customer_sk, region, valid_from, valid_to, is_current FROM lvl_dim_customer ORDER BY customer_id, valid_from"))

# COMMAND ----------

display(spark.sql(MERGE_SQL))   # re-run the same batch

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** **4 rows**. Customer 1 has two versions (NA closed on 06-01, APAC current), customer 2 has one, and customer 3 is new. Re-running affects **0 rows**: the load is
# MAGIC **idempotent**, so a retried job can't corrupt history. Say that word in your interview.
# MAGIC
# MAGIC > **Gotcha:** the source must have **one row per key**. Otherwise MERGE fails with *"multiple source rows matched"*. Dedupe first (Lesson 6).

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Point-in-time join
# MAGIC Every fact must join to the dimension version **valid on the fact's date**:
# MAGIC `valid_from <= sale_date AND (valid_to IS NULL OR sale_date < valid_to)`. This is a **half-open** interval `[valid_from, valid_to)`.

# COMMAND ----------

display(dim_hist.orderBy("customer_id", "valid_from"))

# COMMAND ----------

cond = ((sales.customer_id == dim_hist.customer_id)
        & (sales.sale_date >= dim_hist.valid_from)
        & (dim_hist.valid_to.isNull() | (sales.sale_date < dim_hist.valid_to)))
display(sales.join(dim_hist, cond, "left").select(sales["*"], dim_hist.region).orderBy("sale_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Sale 2 is on **2025-02-01**, the exact day customer 10 moved from NA to EMEA. What would happen with `<=` on **both** ends?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** the sale would match **both** versions, so the fact row is **duplicated** and revenue is double-counted. That's a classic fan-out bug.
# MAGIC Half-open intervals guarantee exactly one match. Sale 7 (unknown customer) keeps a NULL region because of the `left` join. In production you'd map it to an
# MAGIC "unknown member" row (sk = -1) instead of dropping it.

# COMMAND ----------

# MAGIC %md ### DEMO: Change Data Feed, so downstream jobs process only what changed

# COMMAND ----------

spark.sql("ALTER TABLE lvl_dim_customer SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
start_version = spark.sql("DESCRIBE HISTORY lvl_dim_customer").agg(F.max("version")).first()[0] + 1
stage_changes([(2, "LATAM", "consumer")], "2025-09-01")
spark.sql(MERGE_SQL)
display(spark.sql(f"""
  SELECT customer_id, region, valid_to, is_current, _change_type, _commit_version
  FROM table_changes('lvl_dim_customer', {start_version})
  ORDER BY customer_id, _change_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** CDF shows the closed row (`update_preimage` / `update_postimage`) and the new version (`insert`). A downstream fact rebuild reads only this, not the whole table.
# MAGIC Pair it with **liquid clustering** (`CLUSTER BY`) for data skipping, and **predictive optimization** for automatic OPTIMIZE/VACUUM/ANALYZE.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (M-8): Dimension snapshot "as of" a date
# MAGIC From `dim_hist`, return each customer's region **as of 2025-02-15**. Columns: **`customer_id`** and **`region`**.

# COMMAND ----------

m8 = None   # TODO

lesson.check("M-8", m8)

# COMMAND ----------

# MAGIC %md
# MAGIC # Class summary
# MAGIC | Lesson | Pattern | Recognize it when... | Complexity |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Sliding window | contiguous range, longest/shortest/max with a condition | O(n) |
# MAGIC | 2 | Prefix sum + map | subarray sums, negatives allowed | O(n) |
# MAGIC | 3 | Binary search on the answer | "minimum X such that ..." with a monotonic check | O(n log range) |
# MAGIC | 4 | Topological sort | dependencies, ordering, cycle detection | O(V + E) |
# MAGIC | 5 | Heap / intervals | top-k / overlapping ranges | O(n log k) / O(n log n) |
# MAGIC | 6 | Window functions | dedupe, rank, period-over-period, running totals | one shuffle per partition key |
# MAGIC | 7 | Plan reading | find `Exchange`; broadcast vs SMJ; skew = one huge partition | |
# MAGIC | 8 | SCD2 MERGE + point-in-time | history; "as it was" reporting | |
# MAGIC
# MAGIC - [ ] All 8 **YOUR TURN** cells show `PASS`
# MAGIC - [ ] You can explain the staged-updates MERGE without looking
# MAGIC
# MAGIC **Next: `21_mid_test`** (~75 minutes, pass mark 85%).
