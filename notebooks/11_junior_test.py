# Databricks notebook source
# MAGIC %md
# MAGIC # Level 1 - JUNIOR test
# MAGIC **Rules:** about 60 minutes. No looking at `dsa/`, `levels/reference.py` or the walkthrough. Talk through your approach out loud as you go.
# MAGIC
# MAGIC Replace each `raise NotImplementedError` with your code, then run the grading cell under it. Run **Run all** at the end for the
# MAGIC scorecard. Pass mark: **85%**.
# MAGIC
# MAGIC | Section | Items |
# MAGIC |---|---|
# MAGIC | Coding | J1-J6 |
# MAGIC | Spark | JS1-JS3 |
# MAGIC | Multiple choice | Q1-Q10 |

# COMMAND ----------

import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)
from pyspark.sql import functions as F
from levels.grader import Scorecard

card = Scorecard("junior", spark)
orders, customers = card.data["orders"], card.data["customers"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coding
# MAGIC ### J1 - Contains duplicate
# MAGIC Return `True` if any value appears at least twice in `nums`. Target: O(n).

# COMMAND ----------

def contains_duplicate(nums):
    raise NotImplementedError

card.grade("J1", contains_duplicate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### J2 - Valid anagram
# MAGIC Return `True` if `b` is a rearrangement of the letters of `a` (case-sensitive). Target: O(n).

# COMMAND ----------

def is_anagram(a, b):
    raise NotImplementedError

card.grade("J2", is_anagram)

# COMMAND ----------

# MAGIC %md
# MAGIC ### J3 - Binary search
# MAGIC `nums` is sorted ascending with distinct values. Return the index of `target`, or `-1`. Target: O(log n). Don't use `bisect` or `.index()`.

# COMMAND ----------

def binary_search(nums, target):
    raise NotImplementedError

card.grade("J3", binary_search)

# COMMAND ----------

# MAGIC %md
# MAGIC ### J4 - Best time to buy and sell
# MAGIC `prices[i]` is the price on day i. Buy once, then sell on a **later** day. Return the max profit, or 0 if no profit is possible. Target: O(n).

# COMMAND ----------

def max_profit(prices):
    raise NotImplementedError

card.grade("J4", max_profit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### J5 - BFS distances
# MAGIC `graph` is a dict `node -> list of neighbors` (directed). Return a dict `node -> number of hops from start` for every node
# MAGIC reachable from `start`, with `start` at 0. Must handle cycles.

# COMMAND ----------

def bfs_levels(graph, start):
    raise NotImplementedError

card.grade("J5", bfs_levels)

# COMMAND ----------

# MAGIC %md
# MAGIC ### J6 - Most common word
# MAGIC Lowercase the text. A word is a run of letters `a-z` (punctuation separates words). Return the most frequent word, using the
# MAGIC **alphabetically smallest** on ties, or `""` if there are no words.

# COMMAND ----------

def most_common_word(text):
    raise NotImplementedError

card.grade("J6", most_common_word)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark
# MAGIC Inputs: `orders(order_id, customer_id, amount, status)` and `customers(customer_id, name, region)`. Run `display(orders)` if you need to look.
# MAGIC Return a DataFrame with **exactly** the listed columns. Row order doesn't matter, and floats are compared to 2 decimals.
# MAGIC
# MAGIC ### JS1 - Delivered revenue by region
# MAGIC Columns: `region`, `revenue`, the sum of `amount` for orders with `status = 'delivered'`, rounded to 2 decimals.
# MAGIC Only regions that have delivered orders.

# COMMAND ----------

js1 = None   # your DataFrame here

card.grade_df("JS1", js1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JS2 - Customers who never ordered
# MAGIC Columns: `customer_id`. Use the right join type and no `collect()`.

# COMMAND ----------

js2 = None

card.grade_df("JS2", js2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JS3 - Order size bucket
# MAGIC Columns: `order_id`, `order_size`, where amount < 50 is `"small"`, 50 to under 150 is `"medium"`, and 150 or more is `"large"`. No UDFs.

# COMMAND ----------

js3 = None

card.grade_df("JS3", js3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple choice
# MAGIC Put the letter in the string, e.g. `card.mcq("Q1", "a")`.
# MAGIC
# MAGIC **Q1.** Which of these is an **action**? a) `filter` b) `withColumn` c) `count` d) `join`
# MAGIC
# MAGIC **Q2.** "Spark transformations are lazy" means: a) they run immediately on each executor b) they only run on the driver c) results are cached automatically d) they build a plan that runs only when an action is called
# MAGIC
# MAGIC **Q3.** Why prefer `F.upper(col)` over a Python UDF that uppercases? a) built-ins run natively (JVM/Photon), are visible to the optimizer, and avoid Python serialization b) UDFs return wrong results c) UDFs aren't allowed on Databricks d) there's no difference
# MAGIC
# MAGIC **Q4.** Main risk of `df.collect()` on a very large table: a) slower writes b) it deletes the source c) every row is pulled to the driver, which can run out of memory d) none
# MAGIC
# MAGIC **Q5.** A fact table mainly holds: a) descriptive attributes only b) numeric measures plus foreign keys to dimensions c) only dates d) raw JSON
# MAGIC
# MAGIC **Q6.** The "grain" of a fact table is: a) the file size b) the number of partitions c) the number of columns d) what one row represents
# MAGIC
# MAGIC **Q7.** With SCD Type 2, when a customer moves region you: a) close the current row and insert a new version b) overwrite the region c) delete the customer d) add a `previous_region` column
# MAGIC
# MAGIC **Q8.** In the medallion architecture, **silver** is: a) raw data as ingested b) an archive c) cleansed, deduplicated, conformed data d) dashboards only
# MAGIC
# MAGIC **Q9.** A `left_anti` join returns: a) rows present in both b) left rows with **no** match on the right c) all rows from both d) right rows with no match
# MAGIC
# MAGIC **Q10.** Delta Lake tables are stored as: a) CSV files b) JSON files c) Parquet files plus a transaction log d) Avro files

# COMMAND ----------

answers = {"Q1": "", "Q2": "", "Q3": "", "Q4": "", "Q5": "", "Q6": "", "Q7": "", "Q8": "", "Q9": "", "Q10": ""}
for q, a in answers.items():
    card.mcq(q, a)

# COMMAND ----------

# MAGIC %md ## Scorecard

# COMMAND ----------

card.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC **Review:** explanations for every multiple-choice question are in `levels/ANSWER_KEY.md`, and solutions are in `levels/reference.py`.
# MAGIC For each miss, re-read the matching walkthrough section, wait a day, and retake *only those questions*.
