"""Slow-vs-fast pairs of Python code. Each pair returns the same result.

Use with dsa.benchmark.compare() (or the 03_complexity_analysis notebook) to
SEE the difference and practice explaining WHY.
"""
import bisect
import heapq
from collections import Counter
from functools import lru_cache
from typing import Callable, Dict, List, Tuple


# 1. Membership: list scan O(n) vs set hash lookup O(1) -------------------------
def common_items_slow(a: List[int], b: List[int]) -> List[int]:
    return [x for x in a if x in b]          # O(len(a) * len(b))


def common_items_fast(a: List[int], b: List[int]) -> List[int]:
    bs = set(b)                               # O(len(b)) once
    return [x for x in a if x in bs]          # O(len(a))


# 2. String building: += can be O(n^2); ''.join is O(n) --------------------------
# Expect a SMALL gap here: CPython resizes a string in place when nothing else
# references it. That's an implementation detail (not PyPy, not guaranteed) -
# join is the portable O(n) answer. Good nuance to mention in an interview.
def build_csv_slow(rows: List[Tuple]) -> str:
    out = ""
    for r in rows:
        out += ",".join(map(str, r)) + "\n"
    return out


def build_csv_fast(rows: List[Tuple]) -> str:
    return "".join(",".join(map(str, r)) + "\n" for r in rows)


# 3. Counting: manual dict vs Counter (C-implemented) ------------------------------
def count_words_slow(words: List[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for w in words:
        if w in counts:
            counts[w] = counts[w] + 1
        else:
            counts[w] = 1
    return counts


def count_words_fast(words: List[str]) -> Dict[str, int]:
    return dict(Counter(words))


# 4. Top-k: full sort O(n log n) vs heap O(n log k) --------------------------------
def top_k_slow(nums: List[int], k: int) -> List[int]:
    return sorted(nums, reverse=True)[:k]


def top_k_fast(nums: List[int], k: int) -> List[int]:
    return heapq.nlargest(k, nums)


# 5. Repeated lookups in sorted data: linear O(n) vs bisect O(log n) ---------------
def rank_lookups_slow(sorted_vals: List[int], queries: List[int]) -> List[int]:
    return [sum(1 for v in sorted_vals if v < q) for q in queries]


def rank_lookups_fast(sorted_vals: List[int], queries: List[int]) -> List[int]:
    return [bisect.bisect_left(sorted_vals, q) for q in queries]


# 6. Overlapping subproblems: exponential recursion vs memoization -----------------
def fib_slow(n: int) -> int:
    return n if n < 2 else fib_slow(n - 1) + fib_slow(n - 2)   # O(2^n)


@lru_cache(maxsize=None)
def fib_fast(n: int) -> int:
    return n if n < 2 else fib_fast(n - 1) + fib_fast(n - 2)   # O(n)


# 7. Joins in pure Python: nested loop vs hash join (what Spark's hash joins do) ---
def join_slow(orders: List[Tuple[int, int]], customers: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    """orders=(order_id, cust_id), customers=(cust_id, name) -> (order_id, name)."""
    return [(o, name) for o, c in orders for cid, name in customers if c == cid]  # O(n*m)


def join_fast(orders: List[Tuple[int, int]], customers: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    lookup = dict(customers)                         # build side (the "broadcast" table)
    return [(o, lookup[c]) for o, c in orders if c in lookup]   # probe side: O(n + m)


PAIRS: Dict[str, Tuple[Callable, Callable]] = {
    "membership (list vs set)": (common_items_slow, common_items_fast),
    "string build (+= vs join)": (build_csv_slow, build_csv_fast),
    "counting (dict vs Counter)": (count_words_slow, count_words_fast),
    "top-k (sort vs heap)": (top_k_slow, top_k_fast),
    "rank lookups (scan vs bisect)": (rank_lookups_slow, rank_lookups_fast),
    "fibonacci (naive vs memo)": (fib_slow, fib_fast),
    "join (nested loop vs hash)": (join_slow, join_fast),
}
