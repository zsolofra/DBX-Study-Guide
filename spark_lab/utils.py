"""Timing, plan inspection, and safe-config helpers."""
import contextlib
import io
import time
from typing import Callable, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def force(df: DataFrame) -> None:
    """Fully execute a DataFrame without collecting it to the driver.

    df.count() is NOT a fair benchmark: the optimizer can prune columns and
    answer from metadata. The 'noop' sink executes the whole plan and discards rows.
    """
    try:
        df.write.format("noop").mode("overwrite").save()
    except Exception:
        # Fallback: hash every column so nothing can be pruned away.
        df.select(F.sum(F.hash(*df.columns)).alias("h")).collect()


def timed(label: str, fn: Callable[[], object], runs: int = 1) -> float:
    """Run fn `runs` times, print and return the best wall time in seconds.

    On serverless, the first run includes warm-up; compare the 2nd+ runs, and
    expect run-to-run noise - talk about relative differences, not absolutes.
    """
    best = float("inf")
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    print(f"{label:<55} {best:8.2f}s")
    return best


def plan_string(df: DataFrame, mode: str = "formatted") -> str:
    """Capture df.explain() output as a string (works on classic and Spark Connect)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode)
    return buf.getvalue()


def plan_has(df: DataFrame, *operators: str) -> Dict[str, bool]:
    """Which physical operators appear in the plan, e.g. plan_has(df, 'BroadcastHashJoin', 'SortMergeJoin')."""
    p = plan_string(df)
    return {op: op in p for op in operators}


def try_set_conf(spark: SparkSession, key: str, value: str) -> bool:
    """Set a Spark conf if the compute allows it. Serverless only permits a small allow-list."""
    try:
        spark.conf.set(key, value)
        print(f"set {key}={value}")
        return True
    except Exception as e:  # noqa: BLE001 - we just want to report it
        print(f"cannot set {key} on this compute ({type(e).__name__}); serverless manages it for you")
        return False


def get_conf(spark: SparkSession, keys: List[str]) -> Dict[str, str]:
    out = {}
    for k in keys:
        try:
            out[k] = spark.conf.get(k)
        except Exception:
            out[k] = "<not readable>"
    return out


def partition_sizes(df: DataFrame) -> DataFrame:
    """Rows per Spark partition. Uses spark_partition_id() because RDD APIs aren't on serverless."""
    return (df.groupBy(F.spark_partition_id().alias("partition_id"))
              .count()
              .orderBy(F.desc("count")))
