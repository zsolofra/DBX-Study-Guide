"""SCD Type 2 on Delta Lake with a single MERGE (the 'staged updates' pattern).

Why one MERGE? It's atomic: readers never see the old row closed without the
new row inserted. Change detection uses a row hash so you compare one column
instead of N, and NULL-safe comparison is built in.
"""
from typing import Sequence


def row_hash_expr(cols: Sequence[str]) -> str:
    """SQL expression hashing tracked columns. coalesce + delimiter avoids ('a','bc') == ('ab','c') and NULL issues."""
    parts = ", ".join(f"coalesce(cast({c} as string), '~null~')" for c in cols)
    return f"sha2(concat_ws('||', {parts}), 256)"


def scd2_merge_sql(target: str, source: str, key: str, tracked: Sequence[str],
                   effective_col: str = "effective_date") -> str:
    """Build the MERGE statement.

    target: dim table with columns  <key>, <tracked...>, row_hash, valid_from, valid_to, is_current
            (+ an IDENTITY surrogate key column that MERGE fills automatically)
    source: table/view with <key>, <tracked...>, <effective_col>; ONE row per key (dedupe first!)
    """
    h = row_hash_expr([f"s.{c}" for c in tracked])
    cols = [key, *tracked]
    insert_cols = ", ".join(cols + ["row_hash", "valid_from", "valid_to", "is_current"])
    insert_vals = ", ".join([f"u.{c}" for c in cols] + ["u.row_hash", f"u.{effective_col}", "NULL", "true"])
    src_cols = ", ".join(f"s.{c}" for c in cols)
    return f"""
MERGE INTO {target} AS t
USING (
  -- (1) every source row, keyed normally: closes the current version if it changed,
  --     or inserts brand-new keys
  SELECT s.{key} AS merge_key, {src_cols}, s.{effective_col}, {h} AS row_hash
  FROM {source} s
  UNION ALL
  -- (2) changed rows again with a NULL merge key: never match, so they INSERT the new version
  SELECT NULL AS merge_key, {src_cols}, s.{effective_col}, {h} AS row_hash
  FROM {source} s
  JOIN {target} t ON t.{key} = s.{key} AND t.is_current
  WHERE t.row_hash <> {h}
) AS u
ON t.{key} = u.merge_key AND t.is_current
WHEN MATCHED AND t.row_hash <> u.row_hash THEN
  UPDATE SET t.is_current = false, t.valid_to = u.{effective_col}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()


def auto_cdc_example(target: str = "dim_customer", source: str = "customer_changes") -> str:
    """The declarative equivalent in Lakeflow Declarative Pipelines (formerly DLT 'APPLY CHANGES').

    Handles out-of-order events via SEQUENCE BY and deletes via APPLY AS DELETE WHEN.
    Runs inside a pipeline, not a notebook cell.
    """
    return f"""
CREATE OR REFRESH STREAMING TABLE {target};

CREATE FLOW {target}_flow AS AUTO CDC INTO {target}
FROM STREAM({source})
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (operation, change_ts)
STORED AS SCD TYPE 2;
""".strip()
