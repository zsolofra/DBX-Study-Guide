"""Optimization techniques you should be able to write AND explain."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def skew_report(df: DataFrame, key: str, top: int = 10) -> DataFrame:
    """Top keys by row count and their share of the table. A key holding >> 1/num_partitions is skew."""
    total = df.count()
    return (df.groupBy(key).count()
              .withColumn("pct_of_rows", F.round(F.col("count") / F.lit(total) * 100, 3))
              .orderBy(F.desc("count")).limit(top))


def salted_join(fact: DataFrame, dim: DataFrame, key: str, buckets: int = 16, how: str = "inner") -> DataFrame:
    """Spread a hot join key over `buckets` partitions.

    fact side: append salt = random 0..buckets-1  (each hot-key row goes to one of N partitions)
    dim side : replicate each row N times with salt 0..N-1 (so every salted fact row finds its match)
    Cost: dim grows N x - only worth it when dim is too big to broadcast and AQE skew handling isn't enough.
    """
    salted_fact = fact.withColumn("_salt", (F.rand(17) * buckets).cast("int"))
    salted_dim = dim.withColumn("_salt", F.explode(F.sequence(F.lit(0), F.lit(buckets - 1))))
    return salted_fact.join(salted_dim, on=[key, "_salt"], how=how).drop("_salt")


def two_phase_agg(df: DataFrame, key: str, value: str, buckets: int = 16) -> DataFrame:
    """Skew-resistant SUM: partial aggregate on (key, salt), then final aggregate on key."""
    partial = (df.withColumn("_salt", (F.rand(23) * buckets).cast("int"))
                 .groupBy(key, "_salt").agg(F.sum(value).alias("_partial")))
    return partial.groupBy(key).agg(F.sum("_partial").alias(f"sum_{value}"))


def latest_per_key_window(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Dedupe to the newest row per key with ROW_NUMBER - one shuffle, no self-join."""
    w = Window.partitionBy(key).orderBy(F.desc(order_col))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def latest_per_key_selfjoin(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """The anti-pattern: aggregate max then join back (two shuffles, and ties produce duplicates)."""
    latest = df.groupBy(key).agg(F.max(order_col).alias(order_col))
    return df.join(latest, on=[key, order_col], how="inner")


def latest_per_key_maxstruct(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Often fastest: max_by / max(struct) is a hash aggregate with no sort."""
    others = [c for c in df.columns if c != key]
    return (df.groupBy(key)
              .agg(F.max_by(F.struct(*others), F.col(order_col)).alias("_r"))
              .select(key, "_r.*"))


def add_tax_python_udf():
    """Row-at-a-time Python UDF: every row is serialized JVM -> Python -> JVM. Slowest."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    @udf(DoubleType())
    def add_tax(amount, region):
        rate = 0.2 if region == "EMEA" else 0.08
        return float(amount) * (1 + rate) if amount is not None else None

    return add_tax


def add_tax_pandas_udf():
    """Vectorized pandas UDF: Arrow batches, numpy math. Much faster than a plain UDF."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("double")
    def add_tax(amount: pd.Series, region: pd.Series) -> pd.Series:
        rate = (region == "EMEA").map({True: 0.2, False: 0.08})
        return amount * (1 + rate)

    return add_tax


def add_tax_native(amount_col: str = "amount", region_col: str = "region"):
    """Built-in expressions: stays in the JVM/Photon, code-generated, optimizer-visible. Fastest."""
    rate = F.when(F.col(region_col) == "EMEA", 0.2).otherwise(0.08)
    return F.col(amount_col) * (1 + rate)
