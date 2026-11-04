"""Tiny empirical complexity toolkit: time a function at growing n, estimate its Big-O.

    from dsa.benchmark import growth, estimate_exponent
    rows = growth(lambda n: sorted(range(n, 0, -1)), [1_000, 10_000, 100_000])
    estimate_exponent(rows)   # ~1.0-1.1 for n log n

Estimate = slope of log(time) vs log(n). ~0 constant, ~1 linear, ~2 quadratic.
"""
import math
import random
import time
from typing import Callable, Dict, List, Sequence, Tuple

from dsa import python_optimizations as po


def time_call(fn: Callable, *args, repeat: int = 3) -> float:
    """Best-of-`repeat` wall time in seconds (best-of reduces noise from GC/other procs)."""
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn(*args)
        best = min(best, time.perf_counter() - t0)
    return best


def growth(fn_of_n: Callable[[int], object], sizes: Sequence[int], repeat: int = 3) -> List[Tuple[int, float]]:
    """[(n, seconds), ...] for fn_of_n(n). Build inputs OUTSIDE the timed part if setup is costly."""
    return [(n, time_call(fn_of_n, n, repeat=repeat)) for n in sizes]


def estimate_exponent(rows: List[Tuple[int, float]]) -> float:
    """Least-squares slope of log(t) on log(n)."""
    pts = [(math.log(n), math.log(max(t, 1e-9))) for n, t in rows]
    mx = sum(x for x, _ in pts) / len(pts)
    my = sum(y for _, y in pts) / len(pts)
    num = sum((x - mx) * (y - my) for x, y in pts)
    den = sum((x - mx) ** 2 for x, _ in pts)
    return num / den if den else 0.0


def _inputs(n: int) -> Dict[str, tuple]:
    rng = random.Random(42)
    nums = [rng.randint(0, n) for _ in range(n)]
    words = [f"w{rng.randint(0, n // 10 + 1)}" for _ in range(n)]
    return {
        "membership (list vs set)": (nums, nums[: n // 2]),
        "string build (+= vs join)": ([(i, i * 2, "x") for i in range(n)],),
        "counting (dict vs Counter)": (words,),
        "top-k (sort vs heap)": (nums, 10),
        "rank lookups (scan vs bisect)": (sorted(nums), nums[:200]),
        "fibonacci (naive vs memo)": (min(24, 10 + n // 500),),
        "join (nested loop vs hash)": ([(i, i % 500) for i in range(n)], [(c, f"c{c}") for c in range(500)]),
    }


def compare(n: int = 2_000) -> List[Dict[str, object]]:
    """Run every slow/fast pair in python_optimizations at size n; return rows for display()."""
    rows = []
    inputs = _inputs(n)
    for name, (slow, fast) in po.PAIRS.items():
        args = inputs[name]
        po.fib_fast.cache_clear()
        assert slow(*args) == fast(*args), name
        po.fib_fast.cache_clear()
        t_slow = time_call(slow, *args, repeat=1)
        po.fib_fast.cache_clear()
        t_fast = time_call(fast, *args, repeat=1)
        rows.append({
            "technique": name,
            "n": n,
            "slow_ms": round(t_slow * 1000, 3),
            "fast_ms": round(t_fast * 1000, 3),
            "speedup_x": round(t_slow / max(t_fast, 1e-9), 1),
        })
    return rows


if __name__ == "__main__":
    for r in compare():
        print(r)
