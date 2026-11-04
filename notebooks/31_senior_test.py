# Databricks notebook source
# MAGIC %md
# MAGIC # Level 3 - SENIOR test
# MAGIC **Rules:** about 2 hours. No peeking. For the written questions, set a timer for **8 minutes each**, write your answer in the markdown cell,
# MAGIC *then* score it with the rubric in `levels/ANSWER_KEY.md`. Pass mark: **85%**.
# MAGIC
# MAGIC | Section | Items |
# MAGIC |---|---|
# MAGIC | Coding | S1-S5 |
# MAGIC | Spark | SS1-SS3 |
# MAGIC | Multiple choice | Q1-Q6 |
# MAGIC | Written scenarios | W1-W5 (self-scored 0-5) |

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from pyspark.sql import Window, functions as F
from levels.grader import Scorecard

card = Scorecard("senior", spark)
events, change_log, orders = card.data["events"], card.data["change_log"], card.data["orders"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coding
# MAGIC ### S1 - Network delay time
# MAGIC `times` is a list of `[u, v, w]` (a directed edge u -> v that takes w >= 0). Nodes are `1..n`. A signal starts at `k`. Return the time until **all** nodes
# MAGIC have received it, or `-1` if some never do.

# COMMAND ----------

def network_delay_time(times, n, k):
    raise NotImplementedError

card.grade("S1", network_delay_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### S2 - LRU cache
# MAGIC `LRUCache(capacity)` with `get(key)` (returns the value or `-1`) and `put(key, value)`, both **O(1)**. Both `get` and `put` (including updates) count as a use.
# MAGIC When over capacity, evict the least recently used key.

# COMMAND ----------

class LRUCache:
    def __init__(self, capacity):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def put(self, key, value):
        raise NotImplementedError

card.grade("S2", LRUCache)

# COMMAND ----------

# MAGIC %md
# MAGIC ### S3 - Overlapping validity windows
# MAGIC `windows` is a list of `(key, start, end)` where windows are **half-open** `[start, end)` and `end=None` means open-ended. Input is unsorted.
# MAGIC Return a **sorted list of keys** that have at least one pair of overlapping windows.

# COMMAND ----------

def overlapping_keys(windows):
    raise NotImplementedError

card.grade("S3", overlapping_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ### S4 - Minimum concurrent slots
# MAGIC Given half-open job intervals `(start, end)`, return the minimum number of slots needed so that no two jobs overlap in the same slot.
# MAGIC Target: O(n log n).

# COMMAND ----------

def min_concurrent(intervals):
    raise NotImplementedError

card.grade("S4", min_concurrent)

# COMMAND ----------

# MAGIC %md
# MAGIC ### S5 - Entity resolution
# MAGIC `records` is a dict `record_id -> list of identifiers`. Records sharing any identifier belong to the same entity (transitively).
# MAGIC Return the groups as a list of **sorted** lists of record ids, with the outer list sorted too. Records with no identifiers are their own group.

# COMMAND ----------

def resolve_entities(records):
    raise NotImplementedError

card.grade("S5", resolve_entities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark
# MAGIC ### SS1 - Sessionize clickstream
# MAGIC `events(user_id, ts)`. A new session starts at a user's first event, or when the gap since their previous event is **more than** 30 minutes (exactly 30 minutes is still the same session).
# MAGIC Columns: `user_id`, `session_id` (1, 2, 3... per user in time order), `session_start`, `session_end`, `n_events`.

# COMMAND ----------

ss1 = None

card.grade_df("SS1", ss1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SS2 - Rebuild SCD2 from an out-of-order change log
# MAGIC `change_log(customer_id, region, effective_date, ingested_at)`. Rules:
# MAGIC - several rows for the same (customer, effective_date): the **latest `ingested_at`** wins
# MAGIC - a row whose region equals the **previous version's** region (by effective_date) is not a change, so drop it
# MAGIC - `valid_to` = the next version's `valid_from` (exclusive), NULL for the latest; `is_current` = (`valid_to` is NULL)
# MAGIC
# MAGIC Columns: `customer_id`, `region`, `valid_from`, `valid_to`, `is_current`.

# COMMAND ----------

ss2 = None

card.grade_df("SS2", ss2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SS3 - Longest ordering streak
# MAGIC `orders(customer_id, order_date)`, which can have several orders on the same day. For each customer, return the length of the longest run of **consecutive calendar days** with at least one order.
# MAGIC Columns: `customer_id`, `longest_streak`.

# COMMAND ----------

ss3 = None

card.grade_df("SS3", ss3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple choice
# MAGIC **Q1.** AQE's skew-join optimization works by: a) salting keys automatically b) splitting oversized partitions into several tasks, each joined with the matching
# MAGIC partition from the other side c) broadcasting the large side d) dropping skewed keys
# MAGIC
# MAGIC **Q2.** Salting a join key into N buckets costs you: a) a fact side duplicated N times b) nothing c) the other (dimension) side replicated N times d) an extra broadcast of the fact table
# MAGIC
# MAGIC **Q3.** Primary/foreign key constraints on Unity Catalog tables are: a) enforced on every write b) disabled on Delta c) only allowed on views
# MAGIC d) informational, and with `RELY` the optimizer may use them for rewrites
# MAGIC
# MAGIC **Q4.** To make a streaming `foreachBatch` upsert safe to retry: a) MERGE on the business key, and/or pass `txnAppId` + `txnVersion` b) use append mode
# MAGIC c) increase the trigger interval d) delete the checkpoint before each run
# MAGIC
# MAGIC **Q5.** `SELECT count(*) FROM delta_table` is often very fast because: a) the result is always cached b) it samples the data
# MAGIC c) row counts can come from file statistics in the Delta transaction log d) Photon skips counting
# MAGIC
# MAGIC **Q6.** A BI dashboard repeatedly runs the same heavy aggregation over a 20 TB fact table. The best first move: a) serve it from a materialized view or gold
# MAGIC aggregate table refreshed incrementally b) the largest SQL warehouse size c) disable Photon d) export to CSV for the BI tool

# COMMAND ----------

answers = {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": ""}
for q, a in answers.items():
    card.mcq(q, a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Written scenarios (8 minutes each, then self-score 0-5 using `levels/ANSWER_KEY.md`)
# MAGIC Use the framework: clarify -> measure/current state -> options -> recommendation -> risks/validation -> metrics.
# MAGIC
# MAGIC **W1 - Diagnosis.** A stage shows: 400 tasks, median 6 s, max 41 min; the slow task has 38 GB shuffle read and 52 GB spill; the join key is
# MAGIC `customer_id`; the dimension is 3 GB. What's happening, what do you check next, and what do you change?
# MAGIC
# MAGIC *Your answer:*

# COMMAND ----------

# MAGIC %md
# MAGIC **W2 - Warehouse design.** A retailer wants daily sales, inventory, and customer-segment reporting for 300 stores, with history of customer
# MAGIC segment changes, plus near-real-time online orders. Design the model (facts, grain, dimensions, SCD types) and the pipeline on Databricks.
# MAGIC
# MAGIC *Your answer:*

# COMMAND ----------

# MAGIC %md
# MAGIC **W3 - Migration.** A customer has 3,000 Teradata BTEQ scripts, 40 TB, and 600 Tableau dashboards, and their contract ends in 9 months. Plan the migration.
# MAGIC
# MAGIC *Your answer:*

# COMMAND ----------

# MAGIC %md
# MAGIC **W4 - Trade-off.** A 50 TB events table is queried by `event_date` (always) and often by `user_id`. The data engineer wants
# MAGIC `PARTITIONED BY (event_date, user_id)`. Respond.
# MAGIC
# MAGIC *Your answer:*

# COMMAND ----------

# MAGIC %md
# MAGIC **W5 - Customer push-back.** The VP of Analytics says: "Our dashboards were faster on our old warehouse. Databricks SQL is slow." What do you do in the
# MAGIC first two weeks?
# MAGIC
# MAGIC *Your answer:*

# COMMAND ----------

written_scores = {"W1": None, "W2": None, "W3": None, "W4": None, "W5": None}   # 0-5 each, from the rubric
for w, s in written_scores.items():
    card.self_score(w, s, out_of=5)

# COMMAND ----------

card.summary()
