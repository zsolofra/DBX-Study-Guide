# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - DS&A practice & self-test
# MAGIC **Loop:** pick a problem -> open `exercises/<topic>.py` in the workspace file editor -> implement it ->
# MAGIC re-run the test cell with `target = exercises`. Check `dsa/<topic>.py` only AFTER you've tried.
# MAGIC
# MAGIC **Say out loud for every problem (this is what's graded):**
# MAGIC 1. Restate the problem + clarify edge cases (empty input? duplicates? negatives? sorted?)
# MAGIC 2. Brute force + its complexity
# MAGIC 3. The better pattern and *why* it works (the invariant)
# MAGIC 4. Code it, then walk through one example by hand
# MAGIC 5. Final time/space complexity and how it would scale out on Spark

# COMMAND ----------

# MAGIC %pip install -q pytest

# COMMAND ----------

import os, sys, subprocess, random, inspect, importlib
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, REPO_ROOT)

dbutils.widgets.dropdown("target", "exercises", ["exercises", "reference"])
dbutils.widgets.dropdown("topic", "all", ["all", "arrays", "strings", "binary_search", "graphs", "hash_maps", "scd"])

def run_tests(target, topic):
    env = {**os.environ, "DSA_TARGET": target, "PYTHONDONTWRITEBYTECODE": "1"}
    path = "tests" if topic == "all" else f"tests/test_{topic}.py"
    r = subprocess.run([sys.executable, "-m", "pytest", path, "-q", "-p", "no:cacheprovider", "--tb=short"],
                       cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    print(r.stdout[-8000:] or r.stderr[-3000:])

# COMMAND ----------

# MAGIC %md ## Run the tests (re-run after every edit)

# COMMAND ----------

run_tests(dbutils.widgets.get("target"), dbutils.widgets.get("topic"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mock interview: draw a random problem
# MAGIC Set a 25-minute timer. Talk through the 5 steps above before you type.

# COMMAND ----------

TOPICS = ["arrays", "strings", "binary_search", "graphs", "hash_maps", "scd"]
topic = random.choice(TOPICS)
mod = importlib.import_module(f"exercises.{topic}")
fns = [(n, f) for n, f in inspect.getmembers(mod, inspect.isfunction) if f.__module__ == mod.__name__]
name, fn = random.choice(fns)
print(f"TOPIC: {topic}\nPROBLEM: {name}{inspect.signature(fn)}\n")
print(inspect.getdoc(fn))
print(f"\nImplement it in exercises/{topic}.py, then run tests with topic={topic}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Follow-up questions interviewers love (practice answering out loud)
# MAGIC | Problem | Follow-up |
# MAGIC |---|---|
# MAGIC | two_sum | Data doesn't fit in memory? -> sort + two pointers on disk, or hash-partition both sides by value bucket (that's a shuffle join) |
# MAGIC | top_k_frequent | 1 TB of logs? -> `groupBy().count()` then `orderBy().limit(k)`; Spark does a partial top-k per partition (TakeOrderedAndProject) |
# MAGIC | merge_intervals | In Spark? -> window `lag(end)` over sorted starts, flag new groups, running `sum` of flags = group id |
# MAGIC | topological_sort | Where is this in Databricks? -> Jobs task DAGs, Lakeflow pipeline dependency graph |
# MAGIC | LRUCache | Thread safety? Capacity in bytes not items? |
# MAGIC | longest_consecutive | Distributed? -> "gaps and islands": `value - row_number()` is constant within a run |
# MAGIC | scd2_merge | Late-arriving / out-of-order changes? -> sequence column, `AUTO CDC ... SEQUENCE BY` |

# COMMAND ----------

# MAGIC %md ### Bonus: gaps-and-islands in SQL (longest_consecutive, distributed)

# COMMAND ----------

nums = [100, 4, 200, 1, 3, 2, 101, 102, 103, 104]
df = spark.createDataFrame([(n,) for n in set(nums)], "v int")
df.createOrReplaceTempView("nums")
display(spark.sql("""
  WITH g AS (SELECT v, v - row_number() OVER (ORDER BY v) AS grp FROM nums)
  SELECT min(v) AS run_start, max(v) AS run_end, count(*) AS run_len
  FROM g GROUP BY grp ORDER BY run_len DESC
"""))
