# Databricks notebook source
# MAGIC %md
# MAGIC # Level 1: Junior Foundations
# MAGIC **Course:** DBX Study Guide | **Duration:** ~2 hours | **Next:** `11_junior_test`
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this class you will be able to:
# MAGIC 1. Use a repeatable 5-step method to solve any coding question out loud
# MAGIC 2. Pick a **hash map or set** to turn O(n^2) work into O(n)
# MAGIC 3. Solve array problems in **one pass** with running state and two pointers
# MAGIC 4. Write **binary search** without off-by-one bugs
# MAGIC 5. Traverse a graph with **BFS**
# MAGIC 6. Explain Spark's **driver/executor** model, **lazy evaluation**, and the main **join types**
# MAGIC 7. Use the core vocabulary of **data warehousing**: fact, dimension, grain, SCD1/SCD2, medallion
# MAGIC
# MAGIC ## How this class works
# MAGIC | Marker | What to do |
# MAGIC |---|---|
# MAGIC | **CONCEPT** | Read it. The idea comes before the code. |
# MAGIC | **DEMO** | Run the cell, then read the **Breakdown** under it. |
# MAGIC | **KNOWLEDGE CHECK** | Say or write your prediction *before* running the next cell. |
# MAGIC | **YOUR TURN** | Write the code. The cell checks itself. Use `lesson.hint("id")` if you're stuck, or `lesson.solution("id")` to see the answer. |
# MAGIC
# MAGIC > **Tip:** open the **Table of contents** (the list icon in the left sidebar) to jump between lessons. Run cells **top to bottom**,
# MAGIC > using Serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom setup
# MAGIC Run the next cell first. It loads the course libraries and the exercise checker (`lesson`).

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

import time
from collections import Counter, deque

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 1: How to answer any coding question

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: The 5-step method
# MAGIC Interviewers grade **how you think** as much as the final code. Use the same 5 steps every time, out loud:
# MAGIC
# MAGIC | Step | What you say | Why it matters |
# MAGIC |---|---|---|
# MAGIC | 1. Clarify | "Can the input be empty? Sorted? Duplicates? Negatives? What do I return if there's no answer?" | Avoids solving the wrong problem |
# MAGIC | 2. Brute force | "The simple way is ... and it's O(n^2)." | You always have *a* working answer |
# MAGIC | 3. Optimize | "The repeated work is ..., so I'll use a ..." | This is where the points are |
# MAGIC | 4. Code + trace | Write it, then walk through one small example | Catches bugs before the interviewer does |
# MAGIC | 5. Complexity | "O(n) time, O(n) space, and the edge cases are ..." | Shows you understand the trade-off |
# MAGIC
# MAGIC We'll apply it to the most famous interview question: **Two Sum**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Clarify
# MAGIC > *Given a list `nums` and a `target`, return the indices of two numbers that add up to `target`.*
# MAGIC
# MAGIC Good clarifying questions: *Is there always exactly one answer? Can I use the same element twice? Is the list sorted?*
# MAGIC Assume: exactly one answer, the same index can't be used twice, and the list is **not** sorted.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Brute force (DEMO)
# MAGIC Try every pair.

# COMMAND ----------

def two_sum_brute(nums, target):
    for i in range(len(nums)):
        for j in range(i + 1, len(nums)):
            if nums[i] + nums[j] == target:
                return [i, j]
    return []

two_sum_brute([2, 7, 11, 15], 9)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown**
# MAGIC - The outer loop runs n times, and the inner loop runs up to n times, so there are about **n^2 / 2 comparisons**: **O(n^2)** time.
# MAGIC - It uses no extra memory: **O(1)** space.
# MAGIC - The repeated work: for every `nums[i]` we *search* the rest of the list for `target - nums[i]`. **Searching is the expensive part.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Optimize
# MAGIC If searching is the slow part, use a structure where searching is **O(1)**: a **dictionary** (hash map).
# MAGIC As we walk the list, we remember `value -> index` for everything seen so far. For each new number we ask:
# MAGIC *"Have I already seen the number I need?"*

# COMMAND ----------

def two_sum(nums, target):
    seen = {}                       # value -> index
    for i, x in enumerate(nums):
        need = target - x
        if need in seen:            # O(1) average lookup
            return [seen[need], i]
        seen[x] = i                 # store AFTER checking, so x is never paired with itself
    return []

two_sum([2, 7, 11, 15], 9)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Trace by hand
# MAGIC `two_sum([3, 2, 4], 6)`:
# MAGIC
# MAGIC | i | x | need | `seen` before this step | action |
# MAGIC |---|---|---|---|---|
# MAGIC | 0 | 3 | 3 | `{}` | 3 not seen, so store `{3: 0}` |
# MAGIC | 1 | 2 | 4 | `{3: 0}` | 4 not seen, so store `{3: 0, 2: 1}` |
# MAGIC | 2 | 4 | 2 | `{3: 0, 2: 1}` | **2 seen at index 1**, so return `[1, 2]` |
# MAGIC
# MAGIC Notice that index 0 (value 3) is never paired with itself, even though 3 + 3 = 6. That's why we store *after* checking.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Complexity
# MAGIC **O(n) time** (one pass, O(1) lookups) and **O(n) space** (the dict). We traded memory for speed, which is the most common trade-off in interviews.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC For n = 4,000 numbers with **no** answer (the worst case), roughly how many times slower is the brute force? Make a guess, then run the next cell.

# COMMAND ----------

nums = list(range(4000))
for f in (two_sum_brute, two_sum):
    t0 = time.perf_counter()
    f(nums, -1)
    print(f"{f.__name__:<14} {time.perf_counter() - t0:.4f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** usually **hundreds of times slower**. Brute force does about 8,000,000 comparisons, while the dict does 4,000 lookups.
# MAGIC If you double n, the brute force gets **4x** slower but the dict version only **2x**. That's what O(n^2) vs O(n) *means*.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-1): First repeated value
# MAGIC Return the **first value that appears for the second time** as you scan left to right, or `None` if nothing repeats.
# MAGIC `[2, 5, 1, 2, 3, 5]` returns `2` (2 repeats at index 3, before 5 repeats at index 5). Target: **O(n)**.

# COMMAND ----------

def first_repeated(nums):
    # TODO: your code here
    raise NotImplementedError

lesson.check("J-1", first_repeated)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 2: Hash maps, sets, and strings

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Why hash lookups are O(1)
# MAGIC A hash map runs `hash(key)` to compute **which bucket** the key lives in, then looks in that bucket. It doesn't scan everything.
# MAGIC - `set`: stores only keys. Use it to answer "have I seen x?"
# MAGIC - `dict`: stores key -> value pairs. Use it for "where did I see x?" or "how many times?"
# MAGIC - `Counter`: a dict built for counting.
# MAGIC
# MAGIC **Spark connection:** a shuffle sends each row to partition `hash(key) % num_partitions`. It's the same idea, spread across machines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEMO: list vs set membership

# COMMAND ----------

data_list = list(range(200_000))
data_set = set(data_list)
lookups = range(199_000, 200_000)           # 1,000 lookups near the end of the list

t0 = time.perf_counter(); hits = sum(1 for x in lookups if x in data_list); t_list = time.perf_counter() - t0
t0 = time.perf_counter(); hits = sum(1 for x in lookups if x in data_set);  t_set = time.perf_counter() - t0
print(f"list: {t_list:.4f}s   set: {t_set:.6f}s   ({t_list / max(t_set, 1e-9):,.0f}x faster)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** `x in list` checks elements one by one, which is O(n) per lookup. `x in set` jumps straight to one bucket, which is O(1) on average.
# MAGIC The rule of thumb: **if you write `in` inside a loop, the right-hand side should almost always be a set or dict.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEMO: Counting with `Counter`

# COMMAND ----------

c = Counter("mississippi")
print(c)
print("most common:", c.most_common(2))
print("count of 'z':", c["z"])          # a missing key returns 0, no KeyError

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Valid anagram, two ways
# MAGIC Two strings are anagrams if they contain **the same letters with the same counts**.
# MAGIC - **Sort both and compare**: O(n log n).
# MAGIC - **Count both and compare**: O(n). Better, and it's the pattern behind `groupBy().count()`.

# COMMAND ----------

def is_anagram(a, b):
    if len(a) != len(b):          # cheap early exit
        return False
    return Counter(a) == Counter(b)

print(is_anagram("listen", "silent"), is_anagram("rat", "car"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC What does `is_anagram("aacc", "ccac")` return? Count the letters yourself first.

# COMMAND ----------

print(Counter("aacc"), Counter("ccac"), is_anagram("aacc", "ccac"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** `False`. `aacc` has `a:2, c:2` while `ccac` has `c:3, a:1`. They have the same length and the same *set* of letters but different **counts**.
# MAGIC This is why `set(a) == set(b)` is a wrong solution.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-2): First unique character
# MAGIC Return the **index** of the first character that appears exactly once in `s`, or `-1`. `"loveleetcode"` returns `2` (`v`).

# COMMAND ----------

def first_unique_char(s):
    # TODO
    raise NotImplementedError

lesson.check("J-2", first_unique_char)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 3: Arrays and one-pass thinking

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Carry running state instead of looking back
# MAGIC Many array problems seem to need "look at every earlier element", which is O(n^2). Ask yourself:
# MAGIC **what single value about the past do I actually need?** Keep that one value updated as you go.
# MAGIC
# MAGIC > *Best time to buy and sell:* buy once, sell later, and maximize profit. To sell today, you only need **the lowest price so far**.

# COMMAND ----------

# MAGIC %md ### DEMO: max profit, with a trace

# COMMAND ----------

def max_profit(prices, trace=False):
    lowest, best = float("inf"), 0
    for day, p in enumerate(prices):
        lowest = min(lowest, p)
        best = max(best, p - lowest)
        if trace:
            print(f"day {day}: price={p:<3} lowest_so_far={lowest:<3} best_profit={best}")
    return best

max_profit([7, 1, 5, 3, 6, 4], trace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown**
# MAGIC - On day 1 the price is 1, so `lowest` becomes 1. Every later day measures profit against 1.
# MAGIC - On day 4 the price is 6, giving a profit of 5, which is the best.
# MAGIC - We never compared day 4 with days 0, 2 or 3 directly. The running `lowest` summarized them. That makes it **O(n) time and O(1) space**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Two pointers
# MAGIC Put one pointer at each end and move them toward each other. Each step discards at least one element, so the whole scan is O(n).
# MAGIC Classic use: palindromes, and pair sums in a **sorted** list.

# COMMAND ----------

def is_palindrome(s):
    i, j = 0, len(s) - 1
    while i < j:
        if not s[i].isalnum():
            i += 1
        elif not s[j].isalnum():
            j -= 1
        elif s[i].lower() != s[j].lower():
            return False
        else:
            i, j = i + 1, j - 1
    return True

print(is_palindrome("A man, a plan, a canal: Panama"), is_palindrome("race a car"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** skipping non-letters *inside* the loop avoids building a cleaned copy of the string, which saves O(n) extra memory.
# MAGIC Mention this trade-off; interviewers like it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-3): Longest run of 1s
# MAGIC Given a list of 0s and 1s, return the length of the longest run of consecutive 1s. `[1,1,0,1,1,1]` returns `3`. One pass, O(1) space.

# COMMAND ----------

def max_consecutive_ones(nums):
    # TODO
    raise NotImplementedError

lesson.check("J-3", max_consecutive_ones)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 4: Binary search

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Halve the search space
# MAGIC Binary search only works on **sorted** data. Look at the middle element. If it's too small, the answer must be to the right, so discard the left half.
# MAGIC Each step halves the range, giving **O(log n)**: 1,000,000 items take about 20 steps, and 1,000,000,000 take about 30.
# MAGIC
# MAGIC **The invariant (say this out loud):** *"If the target exists, it's always inside `nums[lo..hi]`."* Every update must keep that true.

# COMMAND ----------

# MAGIC %md ### DEMO: watch lo / mid / hi move

# COMMAND ----------

def binary_search(nums, target, trace=False):
    lo, hi = 0, len(nums) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if trace:
            print(f"lo={lo:<3} hi={hi:<3} mid={mid:<3} nums[mid]={nums[mid]}")
        if nums[mid] == target:
            return mid
        if nums[mid] < target:
            lo = mid + 1          # mid is too small, and so is everything left of it
        else:
            hi = mid - 1          # mid is too big, and so is everything right of it
    return -1

nums = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
print("index:", binary_search(nums, 23, trace=True))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** we started with 10 candidates, then 5, then 2, then found it. We used `mid + 1` / `mid - 1` because `mid` itself was already checked.
# MAGIC
# MAGIC ### The three classic bugs
# MAGIC | Bug | Symptom |
# MAGIC |---|---|
# MAGIC | `while lo < hi` with an inclusive `hi` | misses the target when only one candidate is left |
# MAGIC | `lo = mid` instead of `mid + 1` | **infinite loop** when `lo` and `hi` are next to each other |
# MAGIC | searching unsorted data | silently wrong answers |

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC `binary_search([5, 1, 4, 2], 5)`: the list is **not sorted**. Is 5 found? Trace it in your head.

# COMMAND ----------

print(binary_search([5, 1, 4, 2], 5, trace=True))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** it returns `-1` even though 5 is at index 0. At mid=1 it sees 1 < 5 and throws away the left half, which held the 5.
# MAGIC **Always confirm the input is sorted** (or sort it first: O(n log n) once, then each search is O(log n)).

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-4): Search insert position
# MAGIC `nums` is sorted. Return the index of `target` if it's present, otherwise the index where it **would be inserted** to keep the list sorted.
# MAGIC `([1,3,5,6], 2)` returns `1`, and `([1,3,5,6], 7)` returns `4`. Target: O(log n). Don't use `bisect`.

# COMMAND ----------

def search_insert(nums, target):
    # TODO
    raise NotImplementedError

lesson.check("J-4", search_insert)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 5: Graphs and BFS

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Representing a graph
# MAGIC Use an **adjacency list**: a dict from each node to the list of nodes it points to. It's compact (O(V + E)) and makes "who are my neighbors?" O(1).
# MAGIC
# MAGIC A data pipeline is a graph:
# MAGIC ```
# MAGIC raw ──> stg_orders ─────────────┐
# MAGIC  └────> stg_customers ──> dim_customer ──> fact_sales ──> dashboard
# MAGIC ```

# COMMAND ----------

edges = [("raw", "stg_orders"), ("raw", "stg_customers"), ("stg_customers", "dim_customer"),
         ("stg_orders", "fact_sales"), ("dim_customer", "fact_sales"), ("fact_sales", "dashboard")]
graph = {}
for src, dst in edges:
    graph.setdefault(src, []).append(dst)
    graph.setdefault(dst, [])
graph

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Breadth-first search
# MAGIC BFS explores in **rings**: everything 1 hop away, then 2 hops, and so on. That's why it finds the **fewest-hops** path.
# MAGIC - Use a **queue** (`deque`): first in, first out.
# MAGIC - Mark a node visited **when you add it to the queue**, not when you pop it. Otherwise it can be queued twice.

# COMMAND ----------

def bfs_levels(graph, start, trace=False):
    dist = {start: 0}               # doubles as the visited set
    q = deque([start])
    while q:
        node = q.popleft()          # deque.popleft is O(1); list.pop(0) is O(n)
        for nb in graph.get(node, []):
            if nb not in dist:
                dist[nb] = dist[node] + 1
                q.append(nb)
        if trace:
            print(f"visited {node:<14} queue now: {list(q)}")
    return dist

bfs_levels(graph, "raw", trace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** `fact_sales` was first reached from `stg_orders` at distance 2. When `dim_customer` tried to add it again (that path is 3 hops), it was already
# MAGIC in `dist`, so it was skipped. **Complexity: O(V + E)**, since every node is queued once and every edge is checked once.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC What happens on a graph with a **cycle**, `{1: [2], 2: [3], 3: [1]}`, if we *don't* keep a visited set?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** it loops forever (1, 2, 3, 1, 2, ...). The visited set is what guarantees termination.
# MAGIC Real-world version: Unity Catalog lineage graphs and job dependency graphs can contain unexpected cycles, so always track visited nodes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-5): Reachable nodes
# MAGIC Return the **set** of all nodes reachable from `start`, including `start`. BFS or DFS both work. It must handle cycles.

# COMMAND ----------

def reachable(graph, start):
    # TODO
    raise NotImplementedError

lesson.check("J-5", reachable)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 6: PySpark fundamentals

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Driver and executors
# MAGIC ```
# MAGIC            ┌──────────── DRIVER (your notebook) ────────────┐
# MAGIC            │ builds the plan, schedules tasks, gets results │
# MAGIC            └───────────────┬────────────────────────────────┘
# MAGIC        ┌───────────────────┼────────────────────┐
# MAGIC   EXECUTOR 1          EXECUTOR 2           EXECUTOR 3
# MAGIC   partitions 1-4      partitions 5-8       partitions 9-12   <- the data lives and is processed here
# MAGIC ```
# MAGIC A DataFrame is split into **partitions**, which are processed in parallel by executors. The **driver** is one machine. Anything you pull
# MAGIC back to it (`collect()`, `toPandas()`) must fit in its memory.

# COMMAND ----------

d = lesson.data("junior")
orders, customers = d["orders"], d["customers"]
display(orders)

# COMMAND ----------

display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Transformations vs actions (lazy evaluation)
# MAGIC | Transformations: *build the plan* | Actions: *run the plan* |
# MAGIC |---|---|
# MAGIC | `select`, `filter`/`where`, `withColumn`, `join`, `groupBy().agg()`, `orderBy` | `count`, `collect`, `show`, `display`, `write`, `first` |
# MAGIC
# MAGIC Spark waits until an action, then optimizes the **whole** plan at once. For example, it pushes filters down to the file scan so less data is read.

# COMMAND ----------

t0 = time.perf_counter()
big_orders = orders.filter(F.col("amount") > 100).withColumn("is_big", F.lit(True))
print(f"Built the plan in {time.perf_counter() - t0:.4f}s. No data has been processed yet.")

# COMMAND ----------

display(big_orders)    # <- the action: the job actually runs now

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEMO: Peek at the plan Spark built

# COMMAND ----------

big_orders.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** read it **bottom-up**. The scan (here an in-memory `LocalTableScan`, a file scan on a real table) comes first, then `Filter (amount > 100)`, then `Project`
# MAGIC (the `is_big` column). On a Delta table you'd see `PushedFilters` inside the scan, meaning the filter is applied while reading. You'll read much bigger plans at Mid level.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEMO: The everyday API

# COMMAND ----------

display(
    orders
    .filter(F.col("status") == "delivered")                                # WHERE
    .withColumn("amount_with_tax", F.round(F.col("amount") * 1.08, 2))     # derived column
    .select("order_id", "customer_id", "amount", "amount_with_tax")       # SELECT
)

# COMMAND ----------

display(
    orders.groupBy("customer_id")                                          # GROUP BY
          .agg(F.count("*").alias("n_orders"),
               F.round(F.sum("amount"), 2).alias("total"),
               F.max("amount").alias("biggest"))
          .orderBy(F.desc("total"))                                        # ORDER BY
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Join types
# MAGIC | `how=` | Returns | SQL equivalent |
# MAGIC |---|---|---|
# MAGIC | `inner` | rows that match on both sides | `JOIN` |
# MAGIC | `left` | every left row; right columns NULL if there's no match | `LEFT JOIN` |
# MAGIC | `left_semi` | left rows that **have** a match (left columns only, never duplicated) | `WHERE EXISTS` |
# MAGIC | `left_anti` | left rows with **no** match | `WHERE NOT EXISTS` |
# MAGIC | `full` | everything from both sides | `FULL OUTER JOIN` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC There are 6 customers and 10 orders. Customers **5 and 6 have no orders**, and customer 1 has 3. Predict the row count of `customers.join(orders, "customer_id", how)` for each
# MAGIC `how`: inner, left, left_semi, left_anti.

# COMMAND ----------

for how in ["inner", "left", "left_semi", "left_anti"]:
    print(f"{how:<10} -> {customers.join(orders, 'customer_id', how).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer and breakdown**
# MAGIC - **inner = 10**: one row per order (each order has a customer).
# MAGIC - **left = 12**: the 10 matches plus customers 5 and 6 with NULL order columns.
# MAGIC - **left_semi = 4**: customers 1-4, **once each**, even though customer 1 has 3 orders. A semi join never multiplies rows.
# MAGIC - **left_anti = 2**: customers 5 and 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Built-in functions beat Python UDFs
# MAGIC A Python UDF ships **every row** from the JVM to a Python process and back, and the optimizer can't see inside it.
# MAGIC Built-ins (`F.upper`, `F.when`, `F.regexp_replace`, `F.date_add`, ...) run natively and can use Photon.

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("string")
def upper_udf(s):
    return s.upper() if s else None

display(customers.select("name", upper_udf("name").alias("via_udf"), F.upper("name").alias("via_builtin")))

# COMMAND ----------

# MAGIC %md
# MAGIC Same result, but the built-in is much faster at scale. Conditional logic doesn't need a UDF either:

# COMMAND ----------

display(orders.select("order_id", "amount",
                      F.when(F.col("amount") >= 150, "high")
                       .when(F.col("amount") >= 50, "medium")
                       .otherwise("low").alias("tier")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-6): Total amount per status
# MAGIC Build a DataFrame with columns **`status`** and **`total`** (the sum of `amount`, rounded to 2 decimals), one row per status.

# COMMAND ----------

j6 = None   # TODO: orders.groupBy(...)...

lesson.check("J-6", j6)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 7: Data warehousing foundations (your spike)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: OLTP vs OLAP
# MAGIC | | OLTP (the app database) | OLAP (the warehouse) |
# MAGIC |---|---|---|
# MAGIC | Workload | many tiny reads/writes ("update order 42") | few huge reads ("revenue by region by month") |
# MAGIC | Model | normalized (3NF) | dimensional (star schema) |
# MAGIC | Storage | row-oriented | **columnar** (Parquet/Delta): read only the columns you need |
# MAGIC
# MAGIC On Databricks the warehouse is a set of **Delta tables** queried by Spark or Databricks SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Facts, dimensions, grain
# MAGIC ```
# MAGIC                 dim_date
# MAGIC                    │
# MAGIC  dim_customer ── fact_sales ── dim_product        <- a STAR schema: one hop from the fact to every dimension
# MAGIC                    │
# MAGIC                 dim_store
# MAGIC ```
# MAGIC - **Fact table**: one row per *event* (a sale). It holds **measures** (amount, quantity) and **foreign keys** to dimensions. Long and narrow.
# MAGIC - **Dimension**: the *context* (who, what, where, when). Short and wide.
# MAGIC - **Grain**: the exact meaning of ONE fact row, e.g. "one row per order line". **Declare it first.** Mixing grains causes double counting.

# COMMAND ----------

# MAGIC %md ### DEMO: A star-schema query

# COMMAND ----------

orders.createOrReplaceTempView("j_fact_orders")
customers.createOrReplaceTempView("j_dim_customer")

display(spark.sql("""
  SELECT c.region,
         count(*)                AS orders,
         round(sum(o.amount), 2) AS revenue
  FROM j_fact_orders o
  JOIN j_dim_customer c ON o.customer_id = c.customer_id    -- fact -> dimension
  WHERE o.status = 'delivered'
  GROUP BY c.region
  ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** this is the shape of 80% of BI queries: *filter the fact, join to dimensions, group by dimension attributes, aggregate the measures.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Slowly Changing Dimensions
# MAGIC A customer moves from NA to EMEA. What should *last year's* sales report say?
# MAGIC | Type | What happens | Last year's sales show |
# MAGIC |---|---|---|
# MAGIC | **SCD1** | overwrite the region | EMEA (history is rewritten) |
# MAGIC | **SCD2** | close the old row, insert a new version with `valid_from`/`valid_to`/`is_current` | NA (the truth at the time) |

# COMMAND ----------

from datetime import date
from warehouse.scd import scd1_upsert, scd2_merge

before = [{"customer_id": 1, "region": "NA"}]
change = [{"customer_id": 1, "region": "EMEA"}]

print("SCD1 result:")
for row in scd1_upsert(before, change, key="customer_id"):
    print("  ", row)

# COMMAND ----------

dim = scd2_merge([], before, key="customer_id", tracked=["region"], as_of=date(2025, 1, 1))
dim = scd2_merge(dim, change, key="customer_id", tracked=["region"], as_of=date(2025, 6, 1))

print("SCD2 result:")
for row in dim:
    print("  ", row)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown of the SCD2 output**
# MAGIC - `sk=1`: NA, valid from 2025-01-01 **to** 2025-06-01, `is_current=False`.
# MAGIC - `sk=2`: EMEA, valid from 2025-06-01, `valid_to=None` (still open), `is_current=True`.
# MAGIC - `sk` is the **surrogate key**. Facts point at `sk`, not at `customer_id`, so each sale links to the version that was true at the time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC A sale happened on **2025-03-15**. Which `sk` should it point to?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** `sk=1` (NA), because 2025-03-15 falls in `[2025-01-01, 2025-06-01)`. You'll write this "point-in-time" join yourself at Mid level.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Medallion architecture
# MAGIC | Layer | Contents | Typical work |
# MAGIC |---|---|---|
# MAGIC | **Bronze** | raw data, as received (duplicates and all) | ingest with Auto Loader, add ingest metadata |
# MAGIC | **Silver** | cleansed, deduplicated, typed, conformed | dedupe, validate, standardize |
# MAGIC | **Gold** | business-ready: star schemas, aggregates | facts, dimensions, KPIs for BI |

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (J-7): Orders per customer, including customers with none
# MAGIC Columns **`name`** and **`n_orders`**, with **all 6 customers** included (Eli and Fay have `0`).
# MAGIC *Trap:* think carefully about which column you count.

# COMMAND ----------

j7 = None   # TODO

lesson.check("J-7", j7)

# COMMAND ----------

# MAGIC %md
# MAGIC # Class summary
# MAGIC | Lesson | Key idea | Complexity |
# MAGIC |---|---|---|
# MAGIC | 1. Method | clarify, brute force, optimize, trace, complexity | |
# MAGIC | 2. Hash maps | `in set` / dict lookups are O(1); counting replaces sorting | O(n) |
# MAGIC | 3. One pass | carry running state; two pointers from both ends | O(n), O(1) space |
# MAGIC | 4. Binary search | sorted input; keep the invariant; `mid ± 1` | O(log n) |
# MAGIC | 5. BFS | queue + visited set; fewest hops | O(V + E) |
# MAGIC | 6. PySpark | lazy plan, actions run it; join types; built-ins over UDFs | |
# MAGIC | 7. Warehousing | fact/dimension/grain; SCD1 vs SCD2; bronze/silver/gold | |
# MAGIC
# MAGIC ### Before the test, check yourself
# MAGIC - [ ] All 7 **YOUR TURN** cells show `PASS`
# MAGIC - [ ] You can explain *why* each knowledge-check answer is what it is
# MAGIC
# MAGIC **Next: open `11_junior_test`** (~60 minutes, pass mark 85%).
