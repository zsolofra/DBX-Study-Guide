# Data Warehousing Spike - Deep-Dive Study Guide

Your spike is where the interviewer will push deepest. Aim to be able to (1) model it,
(2) build it on Databricks, (3) make it fast, (4) migrate a legacy one to it, and
(5) explain the trade-offs to both an engineer and an exec.

Hands-on companion: `notebooks/04_data_warehousing_lab.ipynb` and `warehouse/`.

---

## 1. Your 60-second "why a lakehouse for DW" pitch

> "A classic EDW couples storage and compute in a proprietary format, so you pay to copy data in,
> you get one engine, and ML/streaming teams make their own copies. On Databricks the warehouse
> lives in open Delta/Iceberg tables in your own cloud storage, governed once in Unity Catalog. Databricks SQL
> serverless warehouses with Photon give you elastic BI concurrency, and the same tables
> serve streaming, ML and AI without copies. Lower TCO, one governance model, no lock-in."

Be ready for the push-back: *"Is it as fast as Snowflake/Teradata for BI?"* -> Talk about Photon
(vectorized C++ engine), Intelligent Workload Management (autoscaling for concurrency), result and
disk caching, liquid clustering + predictive optimization, and point to published TPC-DS results -
but say the right answer is "let's benchmark *your* top 20 queries in a POC."

---

## 2. Dimensional modeling (Kimball) - know these cold

| Concept | One-liner | Example in the lab |
|---|---|---|
| **Grain** | The single most important decision: what does ONE fact row mean? Declare it first. | `fact_sales` = one order line |
| **Fact table** | Numeric measures + FKs to dimensions; long and narrow | `amount`, `quantity` |
| **Dimension** | Descriptive context; short and wide | `dim_customer`, `dim_product`, `dim_date` |
| **Star vs snowflake** | Star = denormalized dims (fewer joins, BI-friendly). Snowflake = normalized dims. Prefer star on Databricks. | |
| **Surrogate key** | Warehouse-owned key, decoupled from source IDs; required for SCD2 | `customer_sk` IDENTITY |
| **Conformed dimension** | Same dim shared by many facts so metrics line up across subject areas | `dim_date` |
| **Degenerate dimension** | A dimension attribute with no table (lives in the fact) | `order_id` |
| **Role-playing dim** | One dim used several ways | `dim_date` as order_date / ship_date |
| **Junk dimension** | Bundle low-cardinality flags into one small dim | status flags |
| **Bridge table** | Resolves many-to-many; watch double counting | customer <-> campaign |

**Fact table types**
- *Transaction* - one row per event (our `fact_sales`). Fully additive.
- *Periodic snapshot* - one row per entity per period (daily inventory, monthly balance). Semi-additive (don't SUM balances across time).
- *Accumulating snapshot* - one row per process instance, updated as milestones hit (order -> ship -> deliver dates). Great fit for Delta MERGE.
- *Factless* - events with no measure (attendance, eligibility).

**Measures**: additive (revenue), semi-additive (balance), non-additive (ratios - store numerator and denominator, compute at query time).

### Slowly Changing Dimensions

| Type | Behavior | When |
|---|---|---|
| 0 | Never change | birth date, original signup channel |
| 1 | Overwrite | typo fixes, attributes nobody reports historically |
| **2** | New row per change, `valid_from/valid_to/is_current` | anything reported "as it was" - region, segment, price tier |
| 3 | Add `previous_value` column | only one level of history needed |
| 4 | Current table + separate history table | very fast-changing attributes |
| 6 | 1+2+3 hybrid: history rows plus a current-value column on every row | "as was" and "as is" reporting from one dim |

**SCD2 on Databricks - three ways (know all three):**
1. **Single Delta MERGE with staged updates** - `warehouse/delta_scd.py`. Atomic, one pass. Key trick: union the source with a copy of changed rows whose merge key is NULL so they fall into `WHEN NOT MATCHED -> INSERT`.
2. **AUTO CDC ... STORED AS SCD TYPE 2** in Lakeflow Declarative Pipelines (formerly DLT `APPLY CHANGES INTO`). Declarative; handles out-of-order data with `SEQUENCE BY` and deletes with `APPLY AS DELETE WHEN`. This is what you'd recommend to a customer.
3. **Pure logic** - `warehouse/scd.py`. Code it in an interview to prove you understand it.

Gotchas to volunteer: dedupe the source first (MERGE fails if multiple source rows match one target row);
use a row hash + NULL-safe comparison; use half-open intervals `[valid_from, valid_to)` so point-in-time
joins never double-match; late-arriving changes need a sequence column; make loads **idempotent**.

---

## 3. Other modeling approaches (be able to compare)

- **Inmon / 3NF EDW** - normalized enterprise model first, marts downstream. Strong integration, slow to deliver.
- **Kimball** - bus architecture of conformed dims, business-process stars. Fast to value. Best fit for gold.
- **Data Vault 2.0** - hubs (business keys), links (relationships), satellites (attributes w/ history). Auditable, parallel loads, schema-change friendly; needs a star "information mart" on top for BI. Hash keys are common here.
- **One Big Table (OBT)** - pre-joined wide table. Fine for a single dashboard with columnar storage + clustering; poor for governance and reuse.

**Medallion mapping:** Bronze = raw/landing (staging area). Silver = cleansed, conformed, often 3NF or Data Vault.
Gold = Kimball stars, aggregates, metric/semantic layer.

---

## 4. Databricks SQL / lakehouse DW features to name-drop correctly

- **SQL warehouses**: Serverless (recommended: instant start, IWM autoscaling), Pro, Classic. Size = per-query power; min/max clusters = concurrency.
- **Photon**: vectorized native engine; big wins on scans, joins, aggregations; no code change.
- **Liquid clustering** (`CLUSTER BY`): replaces Hive partitioning + Z-ORDER; incremental, keys changeable. `CLUSTER BY AUTO` lets Databricks choose keys from query history.
- **Predictive optimization**: automatic OPTIMIZE, VACUUM, ANALYZE for UC managed tables.
- **Data skipping**: per-file min/max stats; only works when data is clustered on filter columns.
- **Constraints**: PK/FK informational (`RELY` lets the optimizer use them); `NOT NULL` and `CHECK` enforced.
- **IDENTITY columns** for surrogate keys.
- **Materialized views & streaming tables**: incremental refresh for gold aggregates.
- **Metric views** / semantic layer in Unity Catalog; **AI/BI dashboards** and **Genie** on top.
- **Caching**: query result cache (warehouse), disk cache (SSD copies of remote Parquet).
- **Query profile**: the BI-side equivalent of the Spark UI - look for files/bytes pruned, spill, time per operator.
- **Unity Catalog governance**: catalogs/schemas, row filters + column masks, lineage, audit, tags, ABAC.
- **Delta time travel / RESTORE / DESCRIBE HISTORY**; **Change Data Feed** for incremental downstream loads.
- **Delta Sharing / Lakehouse Federation** - share without copying; query external DBs in place during migration.
- **SQL scripting** (procedural control flow in SQL) - helps port stored-procedure logic.

---

## 5. Making a warehouse fast - the checklist

1. **Model**: star schema, correct grain, integer surrogate keys, no unnecessary snowflaking.
2. **Layout**: liquid cluster facts on the most common filters (usually date + a high-cardinality FK); right-size files (OPTIMIZE / predictive optimization).
3. **Stats**: `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` so the CBO picks join order and broadcast.
4. **Queries**: filter on raw columns (not `year(col) = 2025` - use a range), project only needed columns, avoid `SELECT *` in views over wide tables, push aggregations into gold/MVs.
5. **Compute**: serverless SQL warehouse, size for the heaviest query, scale out clusters for concurrency.
6. **Measure**: query profile + system tables (`system.query.history`, `system.billing.usage`) to find the top cost/latency queries - optimize the top 10, not everything.

---

## 6. EDW migration playbook (core Delivery Solutions Architect material)

Use this structure in any "how would you migrate X" question:

1. **Discover & assess** - inventory objects, workloads, SLAs, consumers, ETL tools, stored procedures; profile query history to find the hot 20%. Databricks **Lakebridge** (free; profiler/analyzer, SQL converter, reconciler) helps size the effort.
2. **Plan & prioritize** - wave plan by business value and dependencies (that's a **topological sort** of the dependency DAG!). Pick a lighthouse use case. Define success metrics (cost, SLA, adoption).
3. **Land the data** - historical bulk load + CDC for ongoing changes (partner tools, Lakeflow Connect, or Lakehouse Federation to query the source during transition).
4. **Convert code** - automated SQL/ETL conversion (Lakebridge) + manual fixes for procedural logic; refactor into medallion pipelines rather than lift-and-shift where it pays off.
5. **Validate** - reconcile row counts, column-level aggregates, and row hashes between source and target; parallel-run critical reports.
6. **Cut over** - repoint BI tools, run in parallel for N cycles, decommission. Have a rollback plan.
7. **Optimize & govern** - Unity Catalog permissions, clustering, warehouse sizing, cost monitoring with system tables.

Risks to raise proactively: dialect differences (e.g., Teradata `QUALIFY` is fine, but proprietary
functions and implicit casts aren't), stored-procedure-heavy logic, BI tool performance expectations,
data-type precision (DECIMAL scale, timestamps/time zones), and organizational change management.

---

## 7. Practice prompts (answer out loud in 3-5 minutes each)

1. "Design the warehouse for an e-commerce company. Walk me from source systems to the CFO dashboard."
2. "A customer's Power BI dashboard on Databricks SQL takes 40 seconds. How do you diagnose it?"
   (query profile -> scan vs join vs agg time -> pruning? stats? clustering? warehouse size / queueing? BI tool generating bad SQL? result cache hits? -> fixes)
3. "Explain SCD Type 2 and implement it." (then code `scd2_merge` - see `exercises/scd.py`)
4. "We're migrating 4,000 Teradata BTEQ scripts. How would you plan it?"
5. "Partitioning vs Z-ORDER vs liquid clustering - when would you use each?"
6. "How do you handle late-arriving facts and early-arriving facts?"
7. "Kimball or Data Vault for this customer, and why?"
8. "How do you guarantee the fact table load is idempotent and exactly-once?"
   (MERGE on business key, deterministic dedupe, checkpoints for streaming, replay-safe batches)
9. "How do you govern PII in the warehouse?" (UC column masks, row filters, tags, lineage, audit)
10. "The customer asks why they shouldn't just stay on their current cloud data warehouse." (Be balanced, not salesy.)

---

## 8. Tying it back to the coding round

| DS&A topic | Where it shows up in the warehouse |
|---|---|
| Hash maps | Hash joins, hash aggregation, hash partitioning in shuffles, row-hash change detection |
| Binary search | File-level min/max skipping, point-in-time SCD2 lookups, `bisect` over validity windows |
| Sorting / intervals | Sort-merge join, clustering, SCD2 validity windows (no overlaps = merge_intervals) |
| Graphs | Pipeline/job DAGs (topological sort), lineage in Unity Catalog, entity resolution (union-find) |
| Strings | Parsing/cleansing in silver, RLE/dictionary encoding in Parquet |
| Sliding window | SQL window functions, streaming windows + watermarks |
