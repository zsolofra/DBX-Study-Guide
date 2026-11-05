"""Checker for the in-lesson "Your turn" exercises.

    lesson.check("J-1", first_repeated)   # function / class exercises
    lesson.check("J-6", my_df)            # Spark exercises (compared to the reference result)
    lesson.hint("J-1")
    lesson.solution("J-1")                # prints the reference code
"""
import inspect

from levels import lesson_reference as L
from levels.grader import compare_frames, run_cases


def _set_eq(args, got, expected):
    return set(got) == expected


def _run_ops(cls, args):
    (ops,) = args
    obj, out = cls(), []
    for name, *params in ops:
        result = getattr(obj, name)(*params)
        if name != "push":
            out.append(result)
    return out


FUNCTION_EXERCISES = {
    "J-1": {"ref": L.first_repeated, "cases": [(([2, 5, 1, 2, 3, 5],), 2), (([1, 2, 3],), None), (([],), None),
                                                (([7, 7],), 7), (([3, 1, 3, 1],), 3)],
            "hint": "Keep a set of values you've already seen. The first x already in the set is the answer."},
    "J-2": {"ref": L.first_unique_char, "cases": [(("leetcode",), 0), (("loveleetcode",), 2), (("aabb",), -1),
                                                   (("",), -1), (("z",), 0)],
            "hint": "Two passes: count every character with Counter, then return the first index whose count is 1."},
    "J-3": {"ref": L.max_consecutive_ones, "cases": [(([1, 1, 0, 1, 1, 1],), 3), (([0, 0],), 0), (([],), 0),
                                                      (([1],), 1), (([1, 0, 1, 1, 0, 1],), 2)],
            "hint": "Keep a running count that resets to 0 on a zero, and track the best count seen."},
    "J-4": {"ref": L.search_insert, "cases": [(([1, 3, 5, 6], 5), 2), (([1, 3, 5, 6], 2), 1), (([1, 3, 5, 6], 7), 4),
                                               (([1, 3, 5, 6], 0), 0), (([], 3), 0)],
            "hint": "Binary search on the half-open range [0, len(nums)). If nums[mid] < target the answer is right of mid; otherwise mid could be it."},
    "J-5": {"ref": L.reachable, "validator": _set_eq, "cases": [
        (({"a": ["b"], "b": ["c"], "c": ["a"], "d": ["a"]}, "a"), {"a", "b", "c"}),
        (({"a": ["b"], "b": ["c"], "c": ["a"], "d": ["a"]}, "d"), {"a", "b", "c", "d"}),
        (({"x": []}, "x"), {"x"})],
            "hint": "BFS or DFS from start. The visited set IS the answer, and the cycle a -> b -> c -> a must not loop forever."},
    "M-1": {"ref": L.max_sum_window, "cases": [(([1, 4, 2, 10, 23, 3, 1, 0, 20], 4), 39), (([2, 3], 1), 3),
                                                (([-1, -2, -3], 2), -3), (([5, 5, 5], 3), 15)],
            "hint": "Sum the first k elements, then slide: add nums[i] and subtract nums[i-k]. Don't re-sum each window."},
    "M-2": {"ref": L.pivot_index, "cases": [(([1, 7, 3, 6, 5, 6],), 3), (([1, 2, 3],), -1), (([2, 1, -1],), 0),
                                             (([],), -1)],
            "hint": "right_sum = total - left_sum - nums[i]. One pass, keeping left_sum."},
    "M-3": {"ref": L.smallest_divisor, "cases": [(([1, 2, 5, 9], 6), 5), (([44, 22, 33, 11, 1], 5), 44),
                                                  (([2, 3, 5, 7, 11], 11), 3), (([1], 1), 1)],
            "hint": "Bigger divisor means a smaller sum (monotonic). Binary search d in [1, max(nums)] using sum(ceil(n/d)) <= threshold."},
    "M-4": {"ref": L.can_finish, "cases": [((2, [[1, 0]]), True), ((2, [[1, 0], [0, 1]]), False), ((3, []), True),
                                            ((4, [[1, 0], [2, 1], [3, 2], [1, 3]]), False)],
            "hint": "Kahn's algorithm: count the nodes you manage to pop. If that's fewer than n, there's a cycle."},
    "M-5": {"ref": L.kth_largest, "cases": [(([3, 2, 1, 5, 6, 4], 2), 5), (([3, 2, 3, 1, 2, 4, 5, 5, 6], 4), 4),
                                             (([1], 1), 1)],
            "hint": "A MIN-heap of size k: push each number, and pop when the size exceeds k. The root is the k-th largest."},
    "S-1": {"ref": L.cheapest_path_cost, "cases": [
        (([(0, 1, 4), (0, 2, 1), (2, 1, 2), (1, 3, 1), (2, 3, 5)], 0, 3), 4),
        (([(0, 1, 4), (0, 2, 1), (2, 1, 2), (1, 3, 1), (2, 3, 5)], 0, 0), 0),
        (([(0, 1, 4), (0, 2, 1), (2, 1, 2), (1, 3, 1), (2, 3, 5)], 3, 0), -1),
        (([], 0, 1), -1)],
            "hint": "Dijkstra with a min-heap of (cost, node). When dst is popped for the first time, its cost is final."},
    "S-2": {"ref": L.MinStack, "runner": _run_ops, "cases": [
        (([("push", 5), ("push", 2), ("push", 7), ("get_min",), ("pop",), ("get_min",), ("pop",), ("get_min",), ("top",)],),
         [2, 7, 2, 2, 5, 5]),
        (([("push", 3), ("push", 3), ("push", 1), ("pop",), ("get_min",), ("pop",), ("get_min",)],), [1, 3, 3, 3])],
            "hint": "Store (value, min_so_far) on each push. get_min reads the top pair, so it's O(1)."},
    "S-3": {"ref": L.count_groups, "cases": [((5, [(0, 1), (1, 2), (3, 4)]), 2), ((4, []), 4),
                                              ((3, [(0, 1), (1, 2), (2, 0)]), 1), ((1, []), 1)],
            "hint": "Union-find: start with n groups and subtract 1 every time a union joins two DIFFERENT roots."},
}

# id -> (reference fn, dataset level, input names, output columns, hint, required plan operators)
SPARK_EXERCISES = {
    "J-6": (L.total_by_status, "junior", ["orders"], ["status", "total"],
            "groupBy('status').agg(F.round(F.sum('amount'), 2).alias('total'))", []),
    "J-7": (L.orders_per_customer, "junior", ["orders", "customers"], ["name", "n_orders"],
            "LEFT join customers to orders, then count('order_id'). count('*') would give 1 to customers with no orders.", []),
    "M-6": (L.first_order, "mid", ["orders"], ["customer_id", "order_id"],
            "row_number() over (partition by customer_id order by order_date, order_id), then keep rn = 1.", []),
    "M-7": (L.broadcast_current_region, "mid", ["orders", "dim_hist"], ["order_id", "region"],
            "Filter dim_hist to valid_to IS NULL, then join with F.broadcast(...). The plan must show BroadcastHashJoin.",
            ["BroadcastHashJoin"]),
    "M-8": (L.as_of_snapshot, "mid", ["dim_hist"], ["customer_id", "region"],
            "Keep rows where valid_from <= '2025-02-15' AND (valid_to IS NULL OR '2025-02-15' < valid_to).", []),
    "S-4": (L.longest_fail_run, "senior_lesson", ["readings"], ["machine", "longest_fail_readings"],
            "Gaps & islands: flag status changes with lag, running-sum the flags into run_id, keep FAIL runs, count per run, then take the max per machine.", []),
    "S-5": (L.price_as_of, "senior_lesson", ["price_log"], ["sku", "price"],
            "First dedupe (sku, effective_date) to the latest ingested_at. Then keep effective_date <= '2025-03-15' and take the latest per sku.", []),
}


class LessonChecker:
    def __init__(self, spark=None):
        self.spark = spark
        self._data = {}

    def data(self, level):
        if level not in self._data:
            from levels.data import DATASETS
            self._data[level] = DATASETS[level](self.spark)
        return self._data[level]

    def check(self, ex_id, answer):
        if ex_id in FUNCTION_EXERCISES:
            return self._check_function(ex_id, answer)
        return self._check_spark(ex_id, answer)

    def _check_function(self, ex_id, fn):
        spec = FUNCTION_EXERCISES[ex_id]
        result = run_cases(spec, fn)
        if result is None:
            print(f"[TODO] {ex_id}: replace `raise NotImplementedError` with your code, then re-run this cell.")
            return False
        passed, total, note = result
        ok = passed == total
        print(f"[{'PASS' if ok else 'TRY AGAIN'}] {ex_id}: {note}")
        if not ok:
            print(f"      stuck? run lesson.hint('{ex_id}')")
        return ok

    def _check_spark(self, ex_id, df):
        ref_fn, level, inputs, cols, _, needs = SPARK_EXERCISES[ex_id]
        if df is None:
            print(f"[TODO] {ex_id}: assign your DataFrame to the variable, then re-run this cell.")
            return False
        data = self.data(level)
        ok, note = compare_frames(df, lambda: ref_fn(*[data[n] for n in inputs]), cols)
        if ok and needs:
            from spark_lab.utils import plan_string
            plan = plan_string(df)
            absent = [op for op in needs if op not in plan]
            if absent:
                ok, note = False, f"right rows, but the physical plan is missing {absent}"
        print(f"[{'PASS' if ok else 'TRY AGAIN'}] {ex_id}: {note}")
        if not ok:
            print(f"      stuck? run lesson.hint('{ex_id}')")
        return ok

    def hint(self, ex_id):
        if ex_id in FUNCTION_EXERCISES:
            print("HINT:", FUNCTION_EXERCISES[ex_id]["hint"])
        else:
            print("HINT:", SPARK_EXERCISES[ex_id][4])

    def solution(self, ex_id):
        ref = FUNCTION_EXERCISES[ex_id]["ref"] if ex_id in FUNCTION_EXERCISES else SPARK_EXERCISES[ex_id][0]
        print(inspect.getsource(ref))
