# Databricks notebook source
# MAGIC %md
# MAGIC # Level 3 - SENIOR walkthrough
# MAGIC **Goal:** solve harder problems *and* reason about scale, correctness under retries, data that arrives out of order,
# MAGIC and trade-offs. At senior level the interviewer cares as much about **how you decide** as about the code.
# MAGIC
# MAGIC | Part | Topics |
# MAGIC |---|---|
# MAGIC | A | Dijkstra, LRU design, sweep line, union-find, interval overlap checks, thinking in distributed terms |
# MAGIC | B | Skew in depth (salting vs AQE), gaps & islands, sessionization, idempotent writes |
# MAGIC | C | Warehouse correctness: rebuilding SCD2 from a change log, late and early arriving data |
# MAGIC | D | A framework for design and diagnosis answers |
# MAGIC
# MAGIC The examples here use **different data** from `31_senior_test`, so the technique transfers but the answers don't.

# COMMAND ----------

import os, sys, heapq
from collections import OrderedDict, defaultdict
from datetime import date, datetime
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from pyspark.sql import Window, functions as F
from spark_lab import techniques as T
from spark_lab.utils import timed, force

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A - Algorithms with a design angle
# MAGIC ### A1. Dijkstra: shortest paths with weights
# MAGIC BFS only works when every edge costs the same. With **non-negative weights**, always expand the closest unfinished node, using a min-heap.
# MAGIC **Lazy deletion:** don't update entries already in the heap. Push a new one and skip stale entries when they pop. Cost: O((V+E) log V).

# COMMAND ----------

def dijkstra(edges, src):
    g = defaultdict(list)
    for u, v, w in edges:
        g[u].append((v, w))
    dist, heap = {}, [(0, src)]
    while heap:
        d, u = heapq.heappop(heap)
        if u in dist:                # stale entry: u was already finalized with a smaller d
            continue
        dist[u] = d                  # first pop of u is its shortest distance (weights >= 0)
        for v, w in g[u]:
            if v not in dist:
                heapq.heappush(heap, (d + w, v))
    return dist

# Region-to-region replication latency (ms). The direct NA->APAC link is slower than going through EMEA.
links = [("NA", "EMEA", 70), ("EMEA", "APAC", 90), ("NA", "APAC", 200), ("APAC", "LATAM", 150), ("NA", "LATAM", 400)]
print(dijkstra(links, "NA"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Senior follow-ups:** negative weights break it (use Bellman-Ford). "Time until every node is reached" = the **max** of the distances.
# MAGIC Disconnected nodes need explicit handling. On a graph with billions of edges, use iterative relaxation (Pregel, GraphFrames) instead of a single-machine heap.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Designing an LRU cache: O(1) get and put
# MAGIC You need **fast lookup** (a hash map) *and* **fast reordering** (a doubly linked list: move to front, drop from back).
# MAGIC Python's `OrderedDict` is exactly that pair. In the interview, say you know what's inside it. If they ban it, build `Node(prev, next)` plus a dict.

# COMMAND ----------

class TTLLRU:
    """Variant you might be asked for: LRU plus a time-to-live."""
    def __init__(self, cap, ttl):
        self.cap, self.ttl, self.d = cap, ttl, OrderedDict()   # key -> (value, expires_at)

    def get(self, k, now):
        if k not in self.d or self.d[k][1] <= now:
            self.d.pop(k, None)
            return -1
        self.d.move_to_end(k)
        return self.d[k][0]

    def put(self, k, v, now):
        self.d[k] = (v, now + self.ttl)
        self.d.move_to_end(k)                 # an update counts as a use
        while len(self.d) > self.cap:
            self.d.popitem(last=False)        # least recently used is at the front

c = TTLLRU(2, ttl=10)
c.put("a", 1, now=0); c.put("b", 2, now=1); c.get("a", now=2); c.put("c", 3, now=3)
print(c.get("b", now=4), c.get("a", now=5), c.get("a", now=11))   # -1 (evicted), 1, -1 (expired)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 1:** In `put`, why call `move_to_end` even when the key already exists?
# MAGIC
# MAGIC **Answer:** assigning to an existing key in an OrderedDict **keeps its old position**. Without the move, an updated key could be evicted as if it
# MAGIC were stale. This is the most common LRU bug, and the test checks for it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Sweep line: maximum concurrency
# MAGIC Turn each interval into two events, `(start, +1)` and `(end, -1)`. Sort them and keep a running sum. The peak is the maximum overlap. Cost: O(n log n).
# MAGIC With **half-open** intervals, an end and a start at the same time don't overlap, so sort `-1` before `+1`. (Tuples do this for free.)
# MAGIC
# MAGIC *Use:* "how many concurrent job slots or warehouse clusters does this schedule need?"

# COMMAND ----------

jobs = {"ingest": (0, 4), "silver": (3, 7), "gold": (7, 9), "ml_features": (2, 8), "export": (8, 10)}
events = sorted([(s, 1, n) for n, (s, e) in jobs.items()] + [(e, -1, n) for n, (s, e) in jobs.items()])
cur = peak = 0
for t, delta, name in events:
    cur += delta
    peak = max(peak, cur)
    print(f"t={t:<2} {'start' if delta > 0 else 'end  '} {name:<12} running={cur}")
print("peak concurrency:", peak)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A4. Union-find: entity resolution
# MAGIC Records that share **any** identifier (email, phone, device) are the same entity, and this is transitive. Union-find merges groups in about O(1) amortized time
# MAGIC (path compression + union by size). The trick is to map **identifier -> first record that owned it**, then union each later record with that owner.

# COMMAND ----------

def find(parent, x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]    # path halving
        x = parent[x]
    return x

devices = {"d1": ["ip:1"], "d2": ["ip:2", "cookie:9"], "d3": ["cookie:9", "ip:3"], "d4": ["ip:3"], "d5": ["ip:5"]}
parent, owner = {k: k for k in devices}, {}
for rec, ids in devices.items():
    for i in ids:
        if i in owner:
            parent[find(parent, rec)] = find(parent, owner[i])
        else:
            owner[i] = rec
groups = defaultdict(list)
for rec in devices:
    groups[find(parent, rec)].append(rec)
print(sorted(groups.values()))      # d2-d3-d4 linked via cookie:9 and ip:3

# COMMAND ----------

# MAGIC %md
# MAGIC **At scale:** this is connected components. Use GraphFrames `connectedComponents()` or iterative min-label propagation with joins. Mention that a
# MAGIC "super-connector" identifier (a shared office IP) can merge millions of records, so you need to **filter out high-frequency identifiers** first.
# MAGIC That's the same problem as skew.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A5. Validity-window overlap checks (a data-quality test for SCD2)
# MAGIC Sort each key's windows by start. Windows overlap if any start falls **before the previous end** (an open-ended `None` end overlaps everything after it).
# MAGIC The same idea in SQL uses `lag`, so one window function checks a billion-row dimension.

# COMMAND ----------

prices = spark.createDataFrame([
    ("sku1", date(2025, 1, 1), date(2025, 3, 1)), ("sku1", date(2025, 3, 1), None),
    ("sku2", date(2025, 1, 1), date(2025, 5, 1)), ("sku2", date(2025, 4, 1), None),   # overlap!
], "sku string, valid_from date, valid_to date")
w = Window.partitionBy("sku").orderBy("valid_from")
display(prices.withColumn("prev_to", F.lag("valid_to").over(w))
              .withColumn("prev_open", F.lag(F.col("valid_to").isNull()).over(w))
              .filter(F.col("prev_open") | (F.col("valid_from") < F.col("prev_to"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A6. Thinking in distributed terms
# MAGIC | Single-machine idea | At 10B rows |
# MAGIC |---|---|
# MAGIC | hash map count | `groupBy().count()`: partial aggregate per partition, then shuffle by key |
# MAGIC | heap top-k | `orderBy().limit(k)`: each partition keeps its local top-k, then a final merge (TakeOrderedAndProject) |
# MAGIC | sort + sweep | window functions per partition key; avoid a global `ORDER BY` with no `PARTITION BY` |
# MAGIC | BFS / union-find | iterative joins until convergence (each iteration is a shuffle) |
# MAGIC | O(n) loop | O(n/P) per core **only if partitions are balanced**: runtime = the largest partition |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B - Spark at senior depth
# MAGIC ### B1. Skew: salting vs AQE vs broadcast, as a decision

# COMMAND ----------

fact = spark.range(3_000_000).select(
    F.when(F.rand(3) < 0.5, F.lit(0)).otherwise((F.rand(4) * 200_000).cast("long")).alias("k"),
    (F.rand(5) * 100).alias("v"))
dim = spark.range(200_000).select(F.col("id").alias("k"), (F.col("id") % 10).alias("attr"))

timed("sort-merge join on skewed key", lambda: force(fact.join(dim.hint("merge"), "k")), runs=2)
timed("salted join, 16 buckets", lambda: force(T.salted_join(fact, dim, "k", buckets=16)), runs=2)
timed("broadcast join (dim is small)", lambda: force(fact.join(F.broadcast(dim), "k")), runs=2)

# COMMAND ----------

# MAGIC %md
# MAGIC **How to decide, out loud:**
# MAGIC 1. **Can the other side be broadcast?** Then skew barely matters for the join, because the big side never shuffles. That's the cheapest fix.
# MAGIC 2. **Is AQE skew-join enough?** It splits oversized partitions and replicates the matching partition from the other side. It's automatic, but only for sort-merge joins,
# MAGIC    and only when a partition exceeds its thresholds.
# MAGIC 3. **Salting** costs an N-times copy of the other side, plus code complexity. Salt **only the hot keys** (salt = 0 for everything else) to limit that cost.
# MAGIC 4. **For aggregations**, use two-phase aggregation (`T.two_phase_agg`): partial sums on (key, salt), then a final sum.
# MAGIC
# MAGIC Also check whether the hot key is a data-quality issue (NULL or a default value), which you can filter out.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Gaps & islands: the pattern behind sessionization and streaks
# MAGIC **Recipe:** (1) order rows per entity, (2) flag where a *new island starts* (`lag` comparison), (3) take a **running sum of the flags** to get an island id,
# MAGIC (4) `groupBy(entity, island_id)`.
# MAGIC
# MAGIC Example: a machine's status readings. Collapse them into contiguous runs of the same status.

# COMMAND ----------

readings = spark.createDataFrame([
    ("m1", datetime(2025, 5, 1, 8, 0), "OK"), ("m1", datetime(2025, 5, 1, 8, 5), "OK"),
    ("m1", datetime(2025, 5, 1, 8, 10), "FAIL"), ("m1", datetime(2025, 5, 1, 8, 15), "FAIL"),
    ("m1", datetime(2025, 5, 1, 8, 20), "OK"), ("m2", datetime(2025, 5, 1, 8, 0), "FAIL"),
], "machine string, ts timestamp, status string")

w = Window.partitionBy("machine").orderBy("ts")
runs = (readings
        .withColumn("new_run", (F.lag("status").over(w).isNull() | (F.lag("status").over(w) != F.col("status"))).cast("int"))
        .withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy("machine", "run_id", "status")
        .agg(F.min("ts").alias("from_ts"), F.max("ts").alias("to_ts"), F.count("*").alias("readings")))
display(runs.orderBy("machine", "run_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Variant: consecutive integers or dates.** `value - row_number()` is constant within a consecutive run. Here it's applied to ticket numbers:

# COMMAND ----------

tickets = spark.createDataFrame([(t,) for t in [101, 102, 103, 107, 108, 120]], "ticket int")
display(tickets.withColumn("grp", F.col("ticket") - F.row_number().over(Window.orderBy("ticket")))
               .groupBy("grp").agg(F.min("ticket").alias("from"), F.max("ticket").alias("to"), F.count("*").alias("len")))
# For dates: date_sub(order_date, row_number) - deduplicate days first.

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 2:** For **sessionization** (a new session after more than 30 minutes of inactivity), what replaces the "status changed" flag?
# MAGIC
# MAGIC **Answer:** `ts - lag(ts) > 30 minutes`, or a NULL lag for the first event. Compare in seconds (`cast("long")`), and decide whether *exactly* 30 minutes
# MAGIC is a new session. The test specifies it. In streaming, use `session_window()` with a watermark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Idempotent and exactly-once writes
# MAGIC Jobs get retried. A senior answer guarantees **a re-run produces the same table**:
# MAGIC - **MERGE on the business key** instead of append, so a replayed batch updates the same rows instead of duplicating them.
# MAGIC - **`replaceWhere`**: overwrite only the partition or date range this run owns (`df.write.option("replaceWhere", "date = '2025-05-01'")`).
# MAGIC - **Streaming `foreachBatch`**: pass `txnAppId` + `txnVersion` (the batchId) so Delta skips a batch it has already committed. The checkpoint tracks progress.
# MAGIC - **Deterministic logic**: no `rand()` without a seed, no `current_timestamp()` in keys, and tie-breakers in every `row_number`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C - Warehouse correctness
# MAGIC ### C1. Rebuild SCD2 from a full change log (handles late-arriving changes)
# MAGIC An incremental MERGE handles changes that arrive in order. When changes arrive **late** (an effective date earlier than the current version),
# MAGIC the simplest *correct* approach is to **recompute the history for affected keys from the change log**:
# MAGIC 1. Keep one row per (key, effective_date): the **latest ingested** (corrections win).
# MAGIC 2. Drop **no-op** rows where the attribute equals the previous version's (otherwise you create fake versions).
# MAGIC 3. Set `valid_to = lead(valid_from)` and `is_current = valid_to IS NULL`.
# MAGIC
# MAGIC Shown here on product **prices**. The test applies it to customer regions.

# COMMAND ----------

price_log = spark.createDataFrame([
    ("sku1", 10.0, date(2025, 1, 1), datetime(2025, 1, 1)),
    ("sku1", 12.0, date(2025, 4, 1), datetime(2025, 4, 1)),
    ("sku1", 11.0, date(2025, 2, 15), datetime(2025, 4, 3)),   # late: arrives after the 04-01 change
    ("sku1", 11.0, date(2025, 3, 1), datetime(2025, 4, 3)),    # no-op: same as previous version
], "sku string, price double, effective_date date, ingested_at timestamp")

latest = Window.partitionBy("sku", "effective_date").orderBy(F.desc("ingested_at"))
by_key = Window.partitionBy("sku").orderBy("effective_date")
scd2 = (price_log
        .withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
        .withColumn("prev", F.lag("price").over(by_key))
        .filter(F.col("prev").isNull() | (F.col("prev") != F.col("price")))
        .withColumn("valid_to", F.lead("effective_date").over(by_key))
        .select("sku", "price", F.col("effective_date").alias("valid_from"), "valid_to",
                F.col("valid_to").isNull().alias("is_current")))
display(scd2.orderBy("valid_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECKPOINT 3:** Why must the no-op filter run *after* the dedupe and *before* `lead`?
# MAGIC
# MAGIC **Answer:** before dedupe, a correction could look like a no-op against a row that's about to be discarded. After `lead`, the dropped row's dates would
# MAGIC already have been used for the neighbor's `valid_to`, leaving a gap. The order is **dedupe -> collapse -> window**.
# MAGIC
# MAGIC **In production:** `AUTO CDC ... SEQUENCE BY ingested_at STORED AS SCD TYPE 2` in Lakeflow Declarative Pipelines does all of this, including out-of-order data.
# MAGIC After a late change, also **re-key the affected facts**: sales between 02-15 and 03-31 now point to a different version.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Early-arriving facts (the dimension row doesn't exist yet)
# MAGIC Options: (1) **inferred member**: insert a placeholder dimension row (`is_inferred = true`, attributes "Unknown") and SCD1-update it when the real
# MAGIC record lands. (2) Map it to an **unknown member** (sk = -1) and reprocess later. (3) Quarantine the fact.
# MAGIC Never drop it silently, because revenue would no longer reconcile.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D - The senior answer framework
# MAGIC For design and diagnosis questions, use this order so you don't jump straight to a solution:
# MAGIC 1. **Clarify**: volumes, latency/SLA, consumers, concurrency, budget, compliance (PII), existing tools.
# MAGIC 2. **Current state / symptoms**: what's measured? (Spark UI, query profile, system tables.) *Diagnose before you prescribe.*
# MAGIC 3. **Options**: 2-3 approaches with trade-offs (cost, complexity, time to value, operability).
# MAGIC 4. **Recommendation**: pick one and say why, tied back to the constraints.
# MAGIC 5. **Risks and validation**: what could go wrong, how you'll prove it worked (reconciliation, benchmarks), and rollback.
# MAGIC 6. **Success metrics**: runtime, $/run, freshness, adoption.
# MAGIC
# MAGIC ### Worked diagnosis (the shape of answer W1 in the test)
# MAGIC > *"A nightly job went from 40 minutes to 3 hours."*
# MAGIC >
# MAGIC > **Clarify:** what changed (code, data volume, cluster, upstream)? Is it every night or intermittent? **Measure:** Spark UI and query profile. Find the longest stage.
# MAGIC > Compare max vs median task time (skew?), spill (memory?), shuffle bytes (plan change?), number of files scanned (small files / lost pruning?).
# MAGIC > **Hypothesis:** volume doubled and one join is now sort-merge instead of broadcast, *or* a new hot key. **Fix:** chosen from the evidence (broadcast hint,
# MAGIC > AQE settings, salting, OPTIMIZE, clustering). **Validate:** a run-time comparison, plus row-count and aggregate reconciliation. **Prevent:** alerts on job duration,
# MAGIC > and system-table cost dashboards.
# MAGIC
# MAGIC Re-read `docs/data_warehousing_spike.md` sections 5-7 before the test: the written questions draw on them.
# MAGIC
# MAGIC Next: **`31_senior_test`**.
