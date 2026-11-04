# Answer Key - Level Tests (SPOILERS)

Coding and Spark solutions are in `levels/reference.py`. This file explains the multiple-choice answers and gives
**rubrics** for the senior written scenarios. For every miss, write one sentence on *why* the right answer is right. That sentence is what you'll say in the interview.

---

## Junior - multiple choice

| Q | Answer | Why |
|---|---|---|
| Q1 | **c** `count` | Actions trigger execution and return a result or write output. `filter`, `withColumn` and `join` only extend the plan. |
| Q2 | **d** | Lazy evaluation builds a logical plan, and the optimizer (Catalyst) rewrites it as a whole when an action runs, e.g. pushing filters into the scan. |
| Q3 | **a** | Built-ins run in the JVM/Photon with code generation, and the optimizer can see through them. A Python UDF serializes every row to a Python worker and is a black box. |
| Q4 | **c** | `collect()` materializes every row in driver memory. It's fine after an aggregation; it's dangerous on raw tables. |
| Q5 | **b** | Facts hold measures plus foreign keys; dimensions hold descriptive attributes. |
| Q6 | **d** | Grain is the meaning of one row ("one order line"). Declare it before choosing columns. Mixed grain is the root of double counting. |
| Q7 | **a** | SCD2 closes the current version (`valid_to`, `is_current=false`) and inserts a new one. (b) is SCD1, (d) is SCD3. |
| Q8 | **c** | Bronze = raw, Silver = cleansed, deduplicated, conformed, Gold = business-level (stars, aggregates). |
| Q9 | **b** | Anti join = "WHERE NOT EXISTS". Semi join = "WHERE EXISTS". Neither adds right-side columns. |
| Q10 | **c** | Delta = Parquet data files plus a `_delta_log` transaction log (JSON commits and checkpoints), which gives ACID, time travel and file statistics. |

## Mid - multiple choice

| Q | Answer | Why |
|---|---|---|
| Q1 | **b** | `BroadcastExchange` ships the small side to every executor; the large side is joined in place with no shuffle. |
| Q2 | **c** | One straggler with a huge shuffle read is the signature of skew (one hot key, one huge partition). Too few partitions would make *all* tasks slow. |
| Q3 | **a** | MERGE requires at most one source row per target row. Dedupe with `row_number()` over the merge key first. |
| Q4 | **c** | `rank`/`dense_rank` give ties the same number, so duplicates survive. `row_number` plus a tie-breaker gives exactly one deterministic row. |
| Q5 | **b** | Liquid clustering is incremental, keys can change without a rewrite, and it works for high-cardinality columns. Hive partitioning on high cardinality creates millions of tiny files. |
| Q6 | **a** | AQE re-optimizes using runtime statistics: it coalesces partitions, converts sort-merge to broadcast, and splits skewed partitions. |
| Q7 | **b** | Arrow columnar batches plus vectorized pandas/numpy. There's still a JVM<->Python hop, so it's slower than built-ins. |
| Q8 | **c** | Change Data Feed (`table_changes`, `readChangeFeed`) or a streaming read gives you only new or changed rows. |

## Senior - multiple choice

| Q | Answer | Why |
|---|---|---|
| Q1 | **b** | AQE detects partitions larger than the skew thresholds (`skewedPartitionFactor`, `skewedPartitionThresholdInBytes`), splits them, and duplicates the matching other-side partition for each split. |
| Q2 | **c** | Each dimension row is exploded to N salted copies so every salted fact row can find its match. Salt only hot keys to contain the cost. |
| Q3 | **d** | UC PK/FK constraints aren't enforced (NOT NULL and CHECK are). `RELY` tells the optimizer it can trust them, e.g. to eliminate joins. |
| Q4 | **a** | A MERGE keyed on the business key is naturally idempotent. `txnAppId`/`txnVersion` lets Delta skip an already-committed batchId on retry. |
| Q5 | **c** | Delta keeps per-file row counts and min/max stats in the log, so a simple `count(*)` can be answered from metadata. |
| Q6 | **a** | Pre-compute what is repeatedly asked for. A materialized view or gold aggregate cuts work by orders of magnitude. Scaling compute is the costly last resort. |

---

## Senior written scenarios - rubrics (score 0-5)

Give yourself **1 point per bullet** you clearly covered (max 5). Half points are fine. A 4+ is interview-ready.

### W1 - Diagnosis (straggler task with spill)
- Names it: **data skew** on `customer_id` (max/median is about 400x, and one task has most of the shuffle read). The spill is a *consequence* of the oversized partition.
- Next checks: top keys by count (is the hot key NULL, -1, or a test account?), the join strategy in the plan, whether AQE skew join is on, and why it didn't trigger.
- Fix options in order: filter or separately handle a junk hot key; **broadcast** the 3 GB dimension only if executor memory allows (and say why it's borderline: the default threshold is 10 MB, and large broadcasts risk OOM); tune AQE skew thresholds; **salt** the hot keys only.
- Mentions the spill fix as secondary (more or larger partitions, memory-optimized instances), and doesn't just "add more nodes".
- Validates: compares task-time distribution and runtime before and after, reconciles row counts, and adds monitoring.

### W2 - Warehouse design (retail)
- Declares the **grain** of each fact: `fact_sales` = one line per transaction; `fact_inventory_daily` = a **periodic snapshot** per store, product and day (semi-additive: don't sum across days).
- Conformed dimensions: date, store, product, customer. `dim_customer` is **SCD2** for segment (history needed); product price is SCD2 or kept on the fact; corrections are SCD1. Surrogate keys via IDENTITY.
- Pipeline: Auto Loader / Lakeflow Connect into **bronze**; **silver** dedupe, quality expectations and conformance; **gold** stars; `AUTO CDC ... SCD TYPE 2` for customers; streaming tables for online orders (near-real-time), batch for stores.
- Performance: liquid clustering on date plus a frequent filter key, predictive optimization, serverless SQL warehouse, materialized views for the top dashboards.
- Governance and operations: Unity Catalog permissions, PII masking, lineage, data-quality expectations with alerts, idempotent MERGE loads, and late-arriving data handling.

### W3 - Migration (Teradata to Databricks)
- **Assess first**: inventory scripts, objects and dependencies, profile query history to find the critical 20%, size it with Lakebridge's analyzer, and classify the BTEQ by complexity.
- **Wave plan** by business value and dependency graph (a topological order), with a lighthouse use case in the first 6-8 weeks. Use the 9-month deadline to set scope (what's lift-and-shift vs refactor).
- **Data movement**: historical bulk load plus CDC for ongoing changes; Lakehouse Federation for interim access.
- **Code conversion**: automated transpilation (Lakebridge) plus manual work on procedural logic; target medallion pipelines and Jobs; plan for dialect gaps (data types, implicit casts, proprietary functions).
- **Validation and cutover**: automated reconciliation (counts, aggregates, row hashes), parallel runs, repointing Tableau, a performance baseline vs Teradata, a rollback plan, decommissioning, and **change management** (training, an enablement CoE).

### W4 - Partitioning trade-off
- Rejects partitioning by `user_id`: high cardinality means **millions of tiny partitions and files**, huge metadata, slow listing, and bad writes.
- Recommends **liquid clustering** `CLUSTER BY (event_date, user_id)`, or at most date-partitioning only if each date has more than about 1 GB.
- Explains *why it's fast*: file-level min/max stats plus clustering give **data skipping** on both columns; the keys can change later.
- Mentions maintenance: OPTIMIZE or predictive optimization, file sizing, and checking "files pruned" in the query profile.
- Validates with a benchmark on representative queries before and after, and considers write patterns (streaming appends, MERGE).

### W5 - "Databricks SQL is slow" (customer push-back)
- **Listens and quantifies**: which dashboards, which queries, what latency now vs before, at what concurrency? Agrees on success criteria.
- **Measures**: query profile and `system.query.history` for the top N slowest or costliest queries; separates queueing time (warehouse sizing / IWM scaling) from execution time.
- **Fixes by root cause**: BI-generated SQL (`SELECT *`, functions on filter columns), missing clustering or stats, small files, the wrong warehouse type (move to serverless), missing pre-aggregation (MVs), and result cache use.
- Runs a **joint benchmark** on their top 20 queries, shares before/after numbers, and involves Databricks specialists if needed.
- Manages the relationship: weekly check-ins, an executive summary, and a plan that ties performance to the business outcome. Takes ownership rather than getting defensive.
