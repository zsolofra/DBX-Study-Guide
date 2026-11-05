# Databricks notebook source
# MAGIC %md
# MAGIC # Classroom setup
# MAGIC Run by every lesson with `%run ./Includes/Classroom-Setup`. It:
# MAGIC - finds the repo root and puts it on `sys.path` (so `import levels`, `dsa`, `spark_lab`, `warehouse` work)
# MAGIC - imports the usual PySpark names (`F`, `Window`)
# MAGIC - creates the `catalog` / `schema` widgets and the lab schema
# MAGIC - creates `lesson`, the checker for the "Your turn" exercises

# COMMAND ----------

import os
import sys


def _find_repo_root(start):
    path = os.path.abspath(start)
    while True:
        if os.path.isdir(os.path.join(path, "levels")) and os.path.isdir(os.path.join(path, "dsa")):
            return path
        parent = os.path.dirname(path)
        if parent == path:
            raise RuntimeError("Repo root not found. Keep the folder structure from GitHub intact.")
        path = parent


REPO_ROOT = _find_repo_root(os.getcwd())
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from pyspark.sql import Window, functions as F  # noqa: E402
from levels.lessons import LessonChecker  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "interview_prep")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

lesson = LessonChecker(spark)

print(f"Repo root : {REPO_ROOT}")
print(f"Schema    : {CATALOG}.{SCHEMA}")
print("Ready. Exercise helpers: lesson.check(id, answer) | lesson.hint(id) | lesson.solution(id)")
