# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup & smoke test
# MAGIC Run this first in your Databricks Free Edition workspace (serverless compute).
# MAGIC 1. Installs `pytest` into this notebook's environment
# MAGIC 2. Puts the repo root on `sys.path` so `import dsa`, `spark_lab`, `warehouse` work
# MAGIC 3. Runs the reference test suite (should be all green)
# MAGIC 4. Creates the schema the Spark labs write to

# COMMAND ----------

# MAGIC %pip install -q pytest

# COMMAND ----------

import os, sys, subprocess

# Notebooks live in <repo>/notebooks, and the working directory is the notebook's folder.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
print("repo root:", REPO_ROOT)
print(sorted(os.listdir(REPO_ROOT)))

# COMMAND ----------

def run_tests(target="reference", topic=None):
    """Run pytest in a fresh subprocess so edits to your files are always picked up."""
    env = {**os.environ, "DSA_TARGET": target, "PYTHONDONTWRITEBYTECODE": "1"}
    path = f"tests/test_{topic}.py" if topic else "tests"
    r = subprocess.run([sys.executable, "-m", "pytest", path, "-q", "-p", "no:cacheprovider", "--tb=line"],
                       cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    print(r.stdout[-6000:] or r.stderr[-3000:])

run_tests("reference")

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print("Spark version:", spark.version)
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
