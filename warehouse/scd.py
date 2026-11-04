"""Slowly Changing Dimensions in pure Python - the logic behind a Delta MERGE.

Being able to code SCD2 from scratch (and then say "in Databricks I'd use
MERGE or AUTO CDC ... STORED AS SCD TYPE 2") is a strong DW-spike signal.

Dimension row shape (dict):
    natural key column(s) + tracked attribute columns +
    sk (surrogate key, int), valid_from (date), valid_to (date | None), is_current (bool)
"""
import bisect
from datetime import date
from typing import Dict, Hashable, List, Optional, Sequence

Row = Dict[str, object]


def scd1_upsert(dim: List[Row], incoming: List[Row], key: str) -> List[Row]:
    """Type 1: overwrite attributes in place (no history); insert unseen keys.

    Returns a NEW list (inputs are not mutated); existing rows keep their order,
    new rows are appended in incoming order.
    COMPLEXITY: O(d + i) with a key -> position index.
    """
    out = [dict(r) for r in dim]
    pos = {r[key]: i for i, r in enumerate(out)}
    for rec in incoming:
        if rec[key] in pos:
            out[pos[rec[key]]].update(rec)
        else:
            pos[rec[key]] = len(out)
            out.append(dict(rec))
    return out


def scd2_merge(dim: List[Row], incoming: List[Row], key: str,
               tracked: Sequence[str], as_of: date) -> List[Row]:
    """Type 2: keep full history.

    Rules:
      * key not in dim (no current row)  -> insert new version
      * any tracked attribute changed    -> close current row (valid_to = as_of,
                                            is_current = False) AND insert new version
      * nothing changed                  -> no-op (idempotent re-runs!)
      * keys absent from incoming        -> untouched (incremental feed, not a full snapshot)
    New versions get sk = max(existing sk) + 1, +2, ... in incoming order,
    valid_from = as_of, valid_to = None, is_current = True.
    Raise ValueError if incoming has the same key twice (Delta MERGE fails the
    same way - dedupe the source first).
    Returns a NEW list: existing rows (possibly closed) first, then new versions.

    PATTERN: hash map of key -> current row; compare tracked columns.
    COMPLEXITY: O(d + i * t) for d dim rows, i incoming rows, t tracked columns.
    TALK TRACK: "In Delta I'd do this in ONE atomic MERGE by staging each changed
    row twice - once with the real key to close the old version, once with a
    NULL merge key so it falls into WHEN NOT MATCHED and inserts the new one."
    """
    seen_keys = set()
    for rec in incoming:
        if rec[key] in seen_keys:
            raise ValueError(f"duplicate key in source: {rec[key]!r}")
        seen_keys.add(rec[key])

    out = [dict(r) for r in dim]
    current = {r[key]: r for r in out if r["is_current"]}
    next_sk = max((int(r["sk"]) for r in out), default=0) + 1

    for rec in incoming:
        cur = current.get(rec[key])
        if cur is not None and all(cur.get(c) == rec.get(c) for c in tracked):
            continue
        if cur is not None:
            cur["valid_to"] = as_of
            cur["is_current"] = False
        new_row = {key: rec[key], **{c: rec.get(c) for c in tracked},
                   "sk": next_sk, "valid_from": as_of, "valid_to": None, "is_current": True}
        next_sk += 1
        out.append(new_row)
        current[rec[key]] = new_row
    return out


def point_in_time_sk(dim: List[Row], key: str, key_value: Hashable, event_date: date) -> Optional[int]:
    """Surrogate key of the version valid on event_date (valid_from <= d < valid_to), or None.

    This is how a fact row gets the RIGHT dimension version (e.g. the customer's
    region at the time of the sale, not today's region).
    PATTERN: versions sorted by valid_from -> binary search (bisect_right - 1).
    COMPLEXITY: O(v log v) to sort here; O(log v) per lookup once versions are pre-sorted.
    TALK TRACK: "In Spark this is a range join; Databricks has a range-join
    optimization, or you join on key and filter the date window."
    """
    versions = sorted((r for r in dim if r[key] == key_value), key=lambda r: r["valid_from"])
    starts = [r["valid_from"] for r in versions]
    i = bisect.bisect_right(starts, event_date) - 1
    if i < 0:
        return None
    v = versions[i]
    if v["valid_to"] is not None and event_date >= v["valid_to"]:
        return None
    return int(v["sk"])
