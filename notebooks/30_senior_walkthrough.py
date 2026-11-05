# Databricks notebook source
# MAGIC %md
# MAGIC # Level 3: Senior Design, Scale & Correctness
# MAGIC **Course:** DBX Study Guide | **Duration:** ~3 hours | **Prerequisite:** Level 2 | **Next:** `31_senior_test`
# MAGIC
# MAGIC At senior level the interviewer is grading **judgment**: can you solve it, scale it, keep it correct under retries and messy data, and explain the trade-offs?
# MAGIC
# MAGIC ## Learning objectives
# MAGIC 1. Implement **Dijkstra** and explain when BFS isn't enough
# MAGIC 2. **Design** O(1) data structures (LRU cache, min-stack) from first principles
# MAGIC 3. Use a **sweep line** for concurrency and **union-find** for entity resolution
# MAGIC 4. Reason about **distributed cost** and choose between broadcast, AQE and **salting** for skew
# MAGIC 5. Apply **gaps & islands** to runs, streaks and sessions
# MAGIC 6. Build **idempotent** loads and **rebuild SCD2** correctly with late and corrected data
# MAGIC 7. Structure a **senior answer** for design and diagnosis questions
# MAGIC
# MAGIC | Marker | What to do |
# MAGIC |---|---|
# MAGIC | **CONCEPT** | read |
# MAGIC | **DEMO** | run it, then read the **Breakdown** |
# MAGIC | **KNOWLEDGE CHECK** | predict before you run |
# MAGIC | **YOUR TURN** | code it; the cell checks itself (`lesson.hint("id")`, `lesson.solution("id")`) |
# MAGIC
# MAGIC > The examples use **different data** from `31_senior_test`: the techniques transfer, the answers don't.

# COMMAND ----------

# MAGIC %md ## Classroom setup

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

import heapq
from collections import OrderedDict, defaultdict
from spark_lab import techniques as T
from spark_lab.utils import timed, force

d = lesson.data("senior_lesson")
readings, price_log = d["readings"], d["price_log"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 1: Weighted shortest paths (Dijkstra)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT
# MAGIC BFS finds the fewest **hops**. When edges have **different costs**, the fewest hops isn't the cheapest path.
# MAGIC **Dijkstra:** always expand the **cheapest unfinished node next**, using a min-heap of `(cost_so_far, node)`.
# MAGIC With non-negative weights, the first time a node is popped, its cost is final.
# MAGIC
# MAGIC **Lazy deletion:** instead of updating a node's cost inside the heap (which heapq can't do), push a new entry and **skip stale ones** when they pop.

# COMMAND ----------

def dijkstra(edges, src, trace=True):
    g = defaultdict(list)
    for u, v, w in edges:
        g[u].append((v, w))
    dist, heap = {}, [(0, src)]
    while heap:
        cost, u = heapq.heappop(heap)
        if u in dist:
            if trace: print(f"pop ({cost:>3}, {u:<5}) stale, skip")
            continue
        dist[u] = cost
        if trace: print(f"pop ({cost:>3}, {u:<5}) FINAL   heap={sorted(heap)}")
        for v, w in g[u]:
            if v not in dist:
                heapq.heappush(heap, (cost + w, v))
    return dist

# Replication latency between regions (ms): the direct NA->APAC link is slower than going through EMEA.
links = [("NA", "EMEA", 70), ("EMEA", "APAC", 90), ("NA", "APAC", 200), ("APAC", "LATAM", 150), ("NA", "LATAM", 400)]
dijkstra(links, "NA")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown**
# MAGIC - `APAC` was pushed twice: at 200 (direct) and at 160 (via EMEA). The 160 entry popped first and became final. The 200 entry was later skipped as stale.
# MAGIC - `LATAM` = 160 + 150 = 310, beating the direct 400.
# MAGIC - Complexity: **O((V + E) log V)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Why does Dijkstra break with a **negative** edge weight?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** "first pop is final" assumes adding more edges can only make a path *more* expensive. A negative edge discovered later could make an already-finalized node cheaper.
# MAGIC Use **Bellman-Ford** (O(V*E)) instead. Follow-ups to expect: *"time until every node is reached?"* is the **max** of `dist`, and unreachable nodes need handling.
# MAGIC *"A billion edges?"* calls for distributed iterative relaxation (Pregel / GraphFrames).

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (S-1): Cheapest path cost
# MAGIC Edges are `(u, v, cost)` (directed). Return the cheapest cost from `src` to `dst`, or `-1` if it's unreachable. `src == dst` costs 0. Stop as soon as `dst` is final.

# COMMAND ----------

def cheapest_path_cost(edges, src, dst):
    # TODO
    raise NotImplementedError

lesson.check("S-1", cheapest_path_cost)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 2: Designing O(1) data structures

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: An LRU cache is two structures working together
# MAGIC ```
# MAGIC  hash map:  key -> node                (find any node in O(1))
# MAGIC
# MAGIC  doubly linked list (usage order):
# MAGIC  HEAD <-> [least recent] <-> ... <-> [most recent] <-> TAIL
# MAGIC           ^ evict here                ^ move here on every get/put
# MAGIC ```
# MAGIC A list alone makes lookup O(n), and a dict alone can't track order cheaply. Together, everything is O(1). Interviewers often ask you to build it **without** OrderedDict.

# COMMAND ----------

# MAGIC %md ### DEMO, step 1: The node and the list operations

# COMMAND ----------

class Node:
    __slots__ = ("key", "val", "prev", "next")
    def __init__(self, key=None, val=None):
        self.key, self.val, self.prev, self.next = key, val, None, None

def unlink(node):
    node.prev.next, node.next.prev = node.next, node.prev

def append_before_tail(tail, node):
    node.prev, node.next = tail.prev, tail
    tail.prev.next = node
    tail.prev = node

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** `HEAD` and `TAIL` are **sentinel** nodes, dummies that are always present. They remove every "is the list empty?" edge case from `unlink` and `append`.

# COMMAND ----------

# MAGIC %md ### DEMO, step 2: The cache

# COMMAND ----------

class LRUFromScratch:
    def __init__(self, capacity):
        self.cap, self.map = capacity, {}
        self.head, self.tail = Node(), Node()
        self.head.next, self.tail.prev = self.tail, self.head

    def _touch(self, node):                 # mark as most recently used
        unlink(node)
        append_before_tail(self.tail, node)

    def get(self, key):
        if key not in self.map:
            return -1
        self._touch(self.map[key])
        return self.map[key].val

    def put(self, key, val):
        if key in self.map:
            self.map[key].val = val
            self._touch(self.map[key])      # an update counts as a use
            return
        node = Node(key, val)
        self.map[key] = node
        append_before_tail(self.tail, node)
        if len(self.map) > self.cap:
            lru = self.head.next            # least recently used sits right after HEAD
            unlink(lru)
            del self.map[lru.key]

    def order(self):
        out, n = [], self.head.next
        while n is not self.tail:
            out.append(n.key); n = n.next
        return out

# COMMAND ----------

c = LRUFromScratch(2)
for op in [("put", "a", 1), ("put", "b", 2), ("get", "a"), ("put", "c", 3), ("get", "b")]:
    result = getattr(c, op[0])(*op[1:])
    print(f"{str(op):<18} -> {result!s:<5} order (LRU -> MRU): {c.order()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** `get("a")` moved `a` to the most-recent end, so when `c` arrived, **`b`** was the least recently used and got evicted. `get("b")` then returns -1.

# COMMAND ----------

# MAGIC %md ### DEMO, step 3: The same thing with OrderedDict

# COMMAND ----------

class LRU:
    def __init__(self, capacity):
        self.cap, self.d = capacity, OrderedDict()   # an OrderedDict IS a hash map + doubly linked list

    def get(self, key):
        if key not in self.d:
            return -1
        self.d.move_to_end(key)
        return self.d[key]

    def put(self, key, val):
        self.d[key] = val
        self.d.move_to_end(key)
        if len(self.d) > self.cap:
            self.d.popitem(last=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC In the OrderedDict version, what breaks if you delete `self.d.move_to_end(key)` from `put`?

# COMMAND ----------

class BuggyLRU(LRU):
    def put(self, key, val):
        self.d[key] = val                 # move_to_end removed
        if len(self.d) > self.cap:
            self.d.popitem(last=False)

for cls in (LRU, BuggyLRU):
    c = cls(2); c.put(1, 1); c.put(2, 2); c.put(1, 100); c.put(3, 3)
    print(f"{cls.__name__:<9} get(1) -> {c.get(1)}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** assigning to an **existing** key keeps its old position, so the just-updated key `1` was evicted as if it were stale. This is the most common LRU bug, and the test checks for it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (S-2): Min-stack
# MAGIC Design `MinStack` with `push(x)`, `pop()` (returns the value), `top()` and `get_min()`, **all O(1)**. Hint: what extra fact could each element carry?

# COMMAND ----------

class MinStack:
    def __init__(self):
        raise NotImplementedError   # TODO

    def push(self, x):
        raise NotImplementedError

    def pop(self):
        raise NotImplementedError

    def top(self):
        raise NotImplementedError

    def get_min(self):
        raise NotImplementedError

lesson.check("S-2", MinStack)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 3: Sweep line and union-find

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Sweep line
# MAGIC "How many things overlap at the busiest moment?" Turn each interval into two events, `(start, +1)` and `(end, -1)`. Sort them and keep a running total.
# MAGIC The peak is the answer: O(n log n). For **half-open** intervals, an end and a start at the same time don't overlap, so process `-1` first (tuples sort that way).
# MAGIC
# MAGIC **Where you'd use it:** sizing concurrent job slots or SQL warehouse clusters, and finding peak concurrent sessions.

# COMMAND ----------

jobs = {"ingest": (0, 4), "silver": (3, 7), "gold": (7, 9), "ml_features": (2, 8), "export": (8, 10)}
events = sorted([(s, 1, n) for n, (s, e) in jobs.items()] + [(e, -1, n) for n, (s, e) in jobs.items()])
running = peak = 0
for t, delta, name in events:
    running += delta
    peak = max(peak, running)
    print(f"t={t:<2} {'start' if delta > 0 else 'end  '} {name:<12} running={running}")
print("peak concurrency =", peak)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** at t=3, `ingest`, `ml_features` and `silver` are all running, giving a peak of **3**. At t=7, `silver` ends *before* `gold` starts (the -1 sorts first), so it stays at 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Union-find (disjoint sets)
# MAGIC Each item points to a parent, and the **root** names the group. `union(a, b)` links two roots. `find(x)` walks to the root and **compresses the path** as it goes,
# MAGIC giving nearly O(1) per operation.
# MAGIC
# MAGIC **Entity resolution:** device records that share **any** identifier (an IP or a cookie) are the same user, and the link is transitive.

# COMMAND ----------

def find(parent, x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]        # path halving
        x = parent[x]
    return x

devices = {"d1": ["ip:1"], "d2": ["ip:2", "cookie:9"], "d3": ["cookie:9", "ip:3"], "d4": ["ip:3"], "d5": ["ip:5"]}
parent, owner = {k: k for k in devices}, {}
for rec, ids in devices.items():
    for ident in ids:
        if ident in owner:
            print(f"{rec} shares {ident} with {owner[ident]}, so union them")
            parent[find(parent, rec)] = find(parent, owner[ident])
        else:
            owner[ident] = rec

groups = defaultdict(list)
for rec in devices:
    groups[find(parent, rec)].append(rec)
sorted(groups.values())

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** d2 and d3 share `cookie:9`, and d3 and d4 share `ip:3`, so **d2, d3 and d4** are one entity even though d2 and d4 share nothing directly.
# MAGIC
# MAGIC **At scale:** use GraphFrames `connectedComponents()` or iterative label propagation. **Warning:** a "super-connector" identifier (an office IP shared by thousands) merges
# MAGIC unrelated people, so filter out high-frequency identifiers first. That's the same root cause as skew.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (S-3): Count connected groups
# MAGIC Nodes are `0..n-1`, and `pairs` are undirected links. Return the number of groups. Use union-find.

# COMMAND ----------

def count_groups(n, pairs):
    # TODO
    raise NotImplementedError

lesson.check("S-3", count_groups)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 4: Distributed thinking and skew

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: From one machine to a cluster
# MAGIC | Single machine | Distributed equivalent | Hidden cost |
# MAGIC |---|---|---|
# MAGIC | dict count | `groupBy().count()` (partial agg, shuffle, final agg) | shuffle bytes |
# MAGIC | heap top-k | `orderBy().limit(k)`: top-k per partition, then merge | small |
# MAGIC | sort + sweep | window per partition key | a global window = 1 task |
# MAGIC | BFS / union-find | iterative joins until convergence | one shuffle per iteration |
# MAGIC | O(n) loop | O(n/P) per core | **only if partitions are balanced** |
# MAGIC
# MAGIC **The senior insight:** a stage finishes when its **slowest task** finishes. Runtime = the largest partition, not the average.

# COMMAND ----------

# MAGIC %md ### DEMO: Why one hot key ruins parallelism

# COMMAND ----------

from dsa.hash_maps import hash_partition

keys = [f"cust_{i}" for i in range(10_000)] + ["cust_HOT"] * 10_000          # half the rows have one key
sizes = sorted((len(v) for v in hash_partition(keys, 16).values()), reverse=True)
print("rows per partition:", sizes)
print(f"average = {sum(sizes) / 16:,.0f} rows, largest = {sizes[0]:,} rows, so runtime is ~{sizes[0] / (sum(sizes) / 16):.1f}x the balanced case")

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** 15 partitions finish quickly and then sit idle while one partition processes about 10,000 rows. Adding nodes **doesn't help**, because the hot partition still lands on one task.

# COMMAND ----------

# MAGIC %md ### DEMO: Three fixes, side by side

# COMMAND ----------

fact = spark.range(3_000_000).select(
    F.when(F.rand(3) < 0.5, F.lit(0)).otherwise((F.rand(4) * 200_000).cast("long")).alias("k"),
    (F.rand(5) * 100).alias("v"))
dim = spark.range(200_000).select(F.col("id").alias("k"), (F.col("id") % 10).alias("attr"))

timed("sort-merge join on the skewed key", lambda: force(fact.join(dim.hint("merge"), "k")), runs=2)
timed("salted join, 16 buckets", lambda: force(T.salted_join(fact, dim, "k", buckets=16)), runs=2)
timed("broadcast join (dim is small)", lambda: force(fact.join(F.broadcast(dim), "k")), runs=2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown, the decision ladder (say it in this order):**
# MAGIC 1. **Is the hot key junk?** (NULL, -1, "unknown") Filter it out or handle it separately. That's free.
# MAGIC 2. **Can the other side be broadcast?** Then the big side never shuffles and skew barely matters.
# MAGIC 3. **Is AQE skew join enough?** It splits oversized partitions automatically (sort-merge joins, past its thresholds).
# MAGIC 4. **Salt**: add a random 0..N-1 to the fact key and replicate the dimension N times. Salt **only the hot keys** to limit the N-times copy.
# MAGIC 5. **For aggregations**, use two-phase aggregation: sum by (key, salt), then by key (`T.two_phase_agg`).
# MAGIC
# MAGIC Serverless timings are noisy, so compare relative results across runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC You salt a join into **32 buckets**, and the dimension has 50M rows. What did that cost?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** the dimension becomes **1.6 billion rows** (50M x 32) before the join. That's why you salt only hot keys (salt = 0 for normal keys, and only hot keys get
# MAGIC replicated), or prefer AQE or broadcast first.

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 5: Gaps & islands (runs, streaks, sessions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: The 4-step recipe
# MAGIC 1. **Order** rows per entity (window: `partitionBy(entity).orderBy(time)`).
# MAGIC 2. **Flag** where a new island starts (`lag` comparison: the value changed, or the time gap is too big).
# MAGIC 3. **Running-sum the flags** to get an island id.
# MAGIC 4. **Group by** (entity, island id).
# MAGIC
# MAGIC We'll build it one step at a time on machine status readings.

# COMMAND ----------

display(readings.orderBy("machine", "ts"))

# COMMAND ----------

# MAGIC %md ### Step 1: Order and look back with `lag`

# COMMAND ----------

w = Window.partitionBy("machine").orderBy("ts")
step1 = readings.withColumn("prev_status", F.lag("status").over(w))
display(step1.orderBy("machine", "ts"))

# COMMAND ----------

# MAGIC %md ### Step 2: Flag where each run starts

# COMMAND ----------

step2 = step1.withColumn(
    "new_run", (F.col("prev_status").isNull() | (F.col("prev_status") != F.col("status"))).cast("int"))
display(step2.orderBy("machine", "ts"))

# COMMAND ----------

# MAGIC %md ### Step 3: Running sum of the flags = run id

# COMMAND ----------

step3 = step2.withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
display(step3.orderBy("machine", "ts"))

# COMMAND ----------

# MAGIC %md ### Step 4: Collapse each run

# COMMAND ----------

runs = (step3.groupBy("machine", "run_id", "status")
             .agg(F.min("ts").alias("from_ts"), F.max("ts").alias("to_ts"), F.count("*").alias("readings")))
display(runs.orderBy("machine", "run_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** the flag is 1 only on the *first* row of each run, so the running sum stays constant within a run and goes up by 1 at each change.
# MAGIC That constant is the group key. It takes one shuffle (partition by machine) and no self-joins.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Shortcut for consecutive numbers and dates
# MAGIC For consecutive values, `value - row_number()` is **constant within a run**:
# MAGIC ```
# MAGIC ticket:      101 102 103 107 108 120
# MAGIC row_number:    1   2   3   4   5   6
# MAGIC difference:  100 100 100 103 103 114   <- the group key
# MAGIC ```

# COMMAND ----------

tickets = spark.createDataFrame([(t,) for t in [101, 102, 103, 107, 108, 120]], "ticket int")
display(tickets.withColumn("grp", F.col("ticket") - F.row_number().over(Window.orderBy("ticket")))
               .groupBy("grp").agg(F.min("ticket").alias("from"), F.max("ticket").alias("to"), F.count("*").alias("length")))

# COMMAND ----------

# MAGIC %md
# MAGIC For **dates**, deduplicate days first, then use `date_sub(day, row_number)` as the group key.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNOWLEDGE CHECK
# MAGIC **Sessionization:** a new session starts after **more than 30 minutes** of inactivity. Which step of the recipe changes, and how?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:** only **step 2**. The flag becomes `lag(ts) IS NULL OR ts - lag(ts) > 30 minutes`. Compare in seconds with `cast("long")`, and confirm whether *exactly* 30 minutes is
# MAGIC a new session. Steps 3 and 4 are identical. In Structured Streaming, use `session_window()` with a watermark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (S-4): Longest FAIL run per machine
# MAGIC Columns **`machine`** and **`longest_fail_readings`**: the most consecutive `FAIL` readings. Include only machines that have at least one FAIL.

# COMMAND ----------

s4 = None   # TODO

lesson.check("S-4", s4)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 6: Correctness: idempotency and SCD2 with messy data

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Idempotent writes
# MAGIC Jobs **will** be retried. A senior design guarantees that **re-running produces the same table**:
# MAGIC | Technique | When |
# MAGIC |---|---|
# MAGIC | `MERGE` on the business key | upserts: a replayed batch updates the same rows instead of duplicating them |
# MAGIC | `replaceWhere` overwrite | a batch owns a date or partition range: replace exactly that slice |
# MAGIC | `foreachBatch` + `txnAppId`/`txnVersion` | streaming: Delta skips a batchId it has already committed |
# MAGIC | deterministic logic | seeded randomness, tie-breakers in `row_number`, no `current_timestamp()` in keys |

# COMMAND ----------

# MAGIC %md ### DEMO: `replaceWhere`, re-running a day without duplicating it

# COMMAND ----------

day = lambda d_, n: spark.createDataFrame([(d_, i) for i in range(n)], "day string, v int").withColumn("day", F.col("day").cast("date"))

day("2025-05-01", 3).unionByName(day("2025-05-02", 3)).write.mode("overwrite").saveAsTable("lvl_daily")
for attempt in (1, 2):   # the job for 05-02 runs twice (a retry), now with 5 rows
    (day("2025-05-02", 5).write.mode("overwrite")
        .option("replaceWhere", "day = '2025-05-02'").saveAsTable("lvl_daily"))
display(spark.sql("SELECT day, count(*) AS rows FROM lvl_daily GROUP BY day ORDER BY day"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** 05-01 is untouched (3 rows), and 05-02 has **5 rows, not 10**. A plain `append` retry would have doubled it. That's idempotency.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Rebuilding SCD2 from a change log
# MAGIC Incremental MERGE assumes changes arrive **in order**. Real feeds include:
# MAGIC - **late-arriving** changes (the effective date is before the current version)
# MAGIC - **corrections** (two rows with the same effective date, where the latest ingest wins)
# MAGIC - **no-op** rows (the "new" value equals the previous one)
# MAGIC
# MAGIC The simplest *correct* approach is to recompute history for the affected keys: **dedupe, then collapse, then window**. Let's do each step.

# COMMAND ----------

display(price_log.orderBy("sku", "effective_date", "ingested_at"))

# COMMAND ----------

# MAGIC %md ### Step 1: Dedupe, so the latest ingest wins per (key, effective_date)

# COMMAND ----------

latest = Window.partitionBy("sku", "effective_date").orderBy(F.desc("ingested_at"))
dedup = price_log.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")
display(dedup.orderBy("sku", "effective_date"))

# COMMAND ----------

# MAGIC %md **Breakdown:** sku2 on 03-01 keeps **22.0** (ingested 03-10) and drops 25.0. The correction wins.

# COMMAND ----------

# MAGIC %md ### Step 2: Collapse no-op versions

# COMMAND ----------

by_key = Window.partitionBy("sku").orderBy("effective_date")
collapsed = (dedup.withColumn("prev_price", F.lag("price").over(by_key))
                  .filter(F.col("prev_price").isNull() | (F.col("prev_price") != F.col("price"))))
display(collapsed.orderBy("sku", "effective_date"))

# COMMAND ----------

# MAGIC %md **Breakdown:** sku1 on 03-01 (11.0, the same as 02-15) is dropped. Otherwise it would create a fake "change" and split one version into two.

# COMMAND ----------

# MAGIC %md ### Step 3: Derive the validity windows with `lead`

# COMMAND ----------

scd2 = (collapsed.withColumn("valid_to", F.lead("effective_date").over(by_key))
                 .select("sku", "price", F.col("effective_date").alias("valid_from"), "valid_to",
                         F.col("valid_to").isNull().alias("is_current")))
display(scd2.orderBy("sku", "valid_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Breakdown:** the late 02-15 change for sku1 slots into place, `[02-15, 04-01)`, because `lead` uses effective-date order, not arrival order.
# MAGIC
# MAGIC ### KNOWLEDGE CHECK
# MAGIC Why must it be **dedupe -> collapse -> window**, in that order?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer:**
# MAGIC - **Collapse before dedupe:** a correction might be compared against a row that's about to be discarded, so you'd keep or drop the wrong rows.
# MAGIC - **Window before collapse:** a dropped row's date would already have been used as its neighbor's `valid_to`, leaving a gap in history.
# MAGIC
# MAGIC **In production:** `AUTO CDC ... SEQUENCE BY ingested_at STORED AS SCD TYPE 2` (Lakeflow Declarative Pipelines) handles all of this.
# MAGIC After a late change, **re-key the affected facts**, because sales between 02-15 and 03-31 now point to a different version.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: Early-arriving facts
# MAGIC A sale arrives for a customer that isn't in `dim_customer` yet. Your options:
# MAGIC 1. **Inferred member**: insert a placeholder dimension row (`is_inferred = true`, attributes "Unknown"), then SCD1-update it when the real record lands.
# MAGIC 2. **Unknown member** (sk = -1), then reprocess later.
# MAGIC 3. **Quarantine** the fact and alert.
# MAGIC
# MAGIC Never drop it silently, or revenue won't reconcile with the source system.

# COMMAND ----------

# MAGIC %md
# MAGIC ### YOUR TURN (S-5): Price as of a date, from the raw log
# MAGIC From `price_log`, return each sku's **price as of 2025-03-15**. Columns **`sku`** and **`price`**. Apply the correction rule first.
# MAGIC SKUs with nothing effective yet are excluded.

# COMMAND ----------

s5 = None   # TODO

lesson.check("S-5", s5)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lesson 7: How to give a senior answer

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONCEPT: The framework
# MAGIC Design and diagnosis questions are graded on **structure**. Jumping straight to a solution is the most common senior-level mistake.
# MAGIC
# MAGIC | # | Step | Sounds like |
# MAGIC |---|---|---|
# MAGIC | 1 | **Clarify** | "What's the volume, the SLA, who consumes it, any compliance constraints?" |
# MAGIC | 2 | **Measure** | "Before changing anything I'd look at the Spark UI / query profile / system tables for..." |
# MAGIC | 3 | **Options** | "There are 3 ways: A is cheap but..., B..., C..." |
# MAGIC | 4 | **Recommend** | "Given the 2-hour SLA and a small team, I'd pick B because..." |
# MAGIC | 5 | **Risks & validation** | "I'd prove it with reconciliation and a benchmark, and the rollback is..." |
# MAGIC | 6 | **Success metrics** | "Runtime under 30 min, $/run down 40%, freshness < 1 h" |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Worked example: *"A nightly job went from 40 minutes to 3 hours."*
# MAGIC **1. Clarify:** What changed: code, data volume, cluster or runtime, upstream schema? Every night, or only some nights?

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Measure:** In the Spark UI, find the **longest stage**, then compare:
# MAGIC - max vs median task time, which points to **skew**
# MAGIC - spill (memory/disk), which points to **partitions too big**
# MAGIC - shuffle read/write bytes, which points to **a plan change, e.g. broadcast becoming sort-merge**
# MAGIC - files and bytes scanned, which points to **lost pruning or small files**

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Hypotheses** (ranked by evidence): (a) volume doubled, so a dimension crossed the broadcast threshold and the join became sort-merge; (b) a new hot key from
# MAGIC an upstream default value; (c) thousands of small files from a changed write pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Recommend** (matching the evidence): broadcast hint or AQE thresholds for (a); filter/handle the hot key, AQE skew join or salting for (b); OPTIMIZE plus liquid
# MAGIC clustering or optimized writes for (c). **Change one thing at a time.**

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Validate:** compare runtime and the task-time distribution before and after, and reconcile row counts and key aggregates against the previous run.
# MAGIC **6. Prevent:** alerts on job duration, and a `system.lakeflow` / `system.billing` dashboard to catch regressions early.

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRACTICE: Your turn to answer out loud (8 minutes)
# MAGIC > *"Our Databricks SQL dashboards were faster on our old warehouse."*
# MAGIC
# MAGIC Write your answer below using the 6 steps, then compare it with the W5 rubric in `levels/ANSWER_KEY.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC *Your answer:*
# MAGIC 1. Clarify:
# MAGIC 2. Measure:
# MAGIC 3. Options:
# MAGIC 4. Recommend:
# MAGIC 5. Risks & validation:
# MAGIC 6. Success metrics:

# COMMAND ----------

# MAGIC %md
# MAGIC # Class summary
# MAGIC | Lesson | Senior takeaway |
# MAGIC |---|---|
# MAGIC | 1. Dijkstra | min-heap + lazy deletion; non-negative weights; at scale, iterative relaxation |
# MAGIC | 2. Data structure design | combine structures (hash map + linked list); sentinels; updates count as use |
# MAGIC | 3. Sweep line / union-find | events +1/-1 for peak concurrency; transitive grouping; watch for super-connectors |
# MAGIC | 4. Skew | runtime = the largest partition; junk key, broadcast, AQE, then salt (hot keys only) |
# MAGIC | 5. Gaps & islands | order, flag, running sum, group; sessionization changes only the flag |
# MAGIC | 6. Correctness | idempotent writes; SCD2 rebuild = dedupe, collapse, window; never drop early facts |
# MAGIC | 7. Answering | clarify, measure, options, recommend, validate, metrics |
# MAGIC
# MAGIC - [ ] All 5 **YOUR TURN** cells show `PASS`
# MAGIC - [ ] You've written one full practice answer with the framework
# MAGIC - [ ] You've re-read `docs/data_warehousing_spike.md` sections 5-7 (the written test questions draw on them)
# MAGIC
# MAGIC **Next: `31_senior_test`** (~2 hours, pass mark 85%).
