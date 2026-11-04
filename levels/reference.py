"""REFERENCE SOLUTIONS for the level tests. SPOILERS - finish the test first.

Pure-Python solutions are importable without Spark; Spark solutions import
pyspark lazily so local pytest can verify the Python test cases.
"""
import heapq
import re
from collections import Counter, OrderedDict, defaultdict, deque

# ============================== JUNIOR ======================================


def contains_duplicate(nums):
    return len(set(nums)) < len(nums)


def is_anagram(a, b):
    return len(a) == len(b) and Counter(a) == Counter(b)


def binary_search(nums, target):
    lo, hi = 0, len(nums) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if nums[mid] == target:
            return mid
        if nums[mid] < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return -1


def max_profit(prices):
    best, lowest = 0, float("inf")
    for p in prices:
        lowest = min(lowest, p)
        best = max(best, p - lowest)
    return best


def bfs_levels(graph, start):
    dist = {start: 0}
    q = deque([start])
    while q:
        node = q.popleft()
        for nb in graph.get(node, []):
            if nb not in dist:
                dist[nb] = dist[node] + 1
                q.append(nb)
    return dist


def most_common_word(text):
    words = re.findall(r"[a-z]+", text.lower())
    if not words:
        return ""
    counts = Counter(words)
    top = max(counts.values())
    return min(w for w, c in counts.items() if c == top)


# ================================ MID =======================================


def length_of_longest_substring(s):
    last, left, best = {}, 0, 0
    for right, ch in enumerate(s):
        if last.get(ch, -1) >= left:
            left = last[ch] + 1
        last[ch] = right
        best = max(best, right - left + 1)
    return best


def subarray_sum_k(nums, k):
    seen, prefix, total = Counter({0: 1}), 0, 0
    for x in nums:
        prefix += x
        total += seen[prefix - k]
        seen[prefix] += 1
    return total


def min_eating_speed(piles, h):
    lo, hi = 1, max(piles)
    while lo < hi:
        mid = (lo + hi) // 2
        if sum(-(-p // mid) for p in piles) <= h:
            hi = mid
        else:
            lo = mid + 1
    return lo


def course_order(n, prereqs):
    graph, indeg = defaultdict(list), [0] * n
    for course, pre in prereqs:
        graph[pre].append(course)
        indeg[course] += 1
    q = deque(i for i in range(n) if indeg[i] == 0)
    order = []
    while q:
        c = q.popleft()
        order.append(c)
        for nxt in graph[c]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.append(nxt)
    return order if len(order) == n else []


def top_k_frequent_words(words, k):
    counts = Counter(words)
    return [w for w, _ in heapq.nsmallest(k, counts.items(), key=lambda wc: (-wc[1], wc[0]))]


def merge_intervals(intervals):
    out = []
    for s, e in sorted(intervals):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


# ============================== SENIOR ======================================


def network_delay_time(times, n, k):
    graph = defaultdict(list)
    for u, v, w in times:
        graph[u].append((v, w))
    dist = {}
    heap = [(0, k)]
    while heap:
        d, node = heapq.heappop(heap)
        if node in dist:
            continue
        dist[node] = d
        for nb, w in graph[node]:
            if nb not in dist:
                heapq.heappush(heap, (d + w, nb))
    return max(dist.values()) if len(dist) == n else -1


class LRUCache:
    def __init__(self, capacity):
        self.cap, self.data = capacity, OrderedDict()

    def get(self, key):
        if key not in self.data:
            return -1
        self.data.move_to_end(key)
        return self.data[key]

    def put(self, key, value):
        self.data[key] = value
        self.data.move_to_end(key)
        if len(self.data) > self.cap:
            self.data.popitem(last=False)


def overlapping_keys(windows):
    by_key = defaultdict(list)
    for key, start, end in windows:
        by_key[key].append((start, end))
    bad = []
    for key, ws in by_key.items():
        ws.sort()
        for (s1, e1), (s2, _) in zip(ws, ws[1:]):
            if e1 is None or s2 < e1:
                bad.append(key)
                break
    return sorted(bad)


def min_concurrent(intervals):
    events = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals])
    cur = best = 0
    for _, delta in events:  # ends (-1) sort before starts (+1) at the same time: half-open
        cur += delta
        best = max(best, cur)
    return best


def resolve_entities(records):
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    owner = {}
    for rid, idents in records.items():
        find(rid)
        for ident in idents:
            if ident in owner:
                parent[find(rid)] = find(owner[ident])
            else:
                owner[ident] = rid
    groups = defaultdict(list)
    for rid in records:
        groups[find(rid)].append(rid)
    return sorted(sorted(g) for g in groups.values())


# ============================ SPARK (lazy) ==================================


def _F():
    from pyspark.sql import Window, functions as F
    return F, Window


def js1_revenue_by_region(orders, customers):
    F, _ = _F()
    return (orders.filter(F.col("status") == "delivered").join(customers, "customer_id")
                  .groupBy("region").agg(F.round(F.sum("amount"), 2).alias("revenue")))


def js2_customers_without_orders(orders, customers):
    return customers.join(orders, "customer_id", "left_anti").select("customer_id")


def js3_order_size(orders):
    F, _ = _F()
    size = (F.when(F.col("amount") < 50, "small")
             .when(F.col("amount") < 150, "medium").otherwise("large"))
    return orders.select("order_id", size.alias("order_size"))


def ms1_latest_status(raw_orders):
    F, W = _F()
    w = W.partitionBy("order_id").orderBy(F.desc("ingested_at"))
    return (raw_orders.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
                      .select("order_id", "status"))


def ms2_last_3_orders(orders):
    F, W = _F()
    w = W.partitionBy("customer_id").orderBy(F.desc("order_date"), F.desc("order_id"))
    return (orders.withColumn("rn", F.row_number().over(w)).filter("rn <= 3")
                  .select("customer_id", "order_id", "rn"))


def ms3_mom_growth(orders):
    F, W = _F()
    m = (orders.groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
               .agg(F.round(F.sum("amount"), 2).alias("revenue")))
    prev = F.lag("revenue").over(W.orderBy("month"))
    return m.withColumn("mom_pct", F.round((F.col("revenue") / prev - 1) * 100, 2))


def ms4_region_at_sale(sales, dim_hist):
    F, _ = _F()
    cond = ((sales.customer_id == dim_hist.customer_id)
            & (sales.sale_date >= dim_hist.valid_from)
            & (dim_hist.valid_to.isNull() | (sales.sale_date < dim_hist.valid_to)))
    return sales.join(dim_hist, cond, "left").select(sales.sale_id, dim_hist.region)


def ss1_sessionize(events, gap_seconds=1800):
    F, W = _F()
    w = W.partitionBy("user_id").orderBy("ts")
    prev = F.lag("ts").over(w)
    is_new = F.when(prev.isNull() | ((F.col("ts").cast("long") - prev.cast("long")) > gap_seconds), 1).otherwise(0)
    flagged = events.withColumn("is_new", is_new)
    with_id = flagged.withColumn("session_id", F.sum("is_new").over(w.rowsBetween(W.unboundedPreceding, W.currentRow)))
    return (with_id.groupBy("user_id", "session_id")
                   .agg(F.min("ts").alias("session_start"), F.max("ts").alias("session_end"),
                        F.count("*").alias("n_events")))


def ss2_rebuild_scd2(change_log):
    F, W = _F()
    latest = W.partitionBy("customer_id", "effective_date").orderBy(F.desc("ingested_at"))
    dedup = (change_log.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
                       .select("customer_id", "region", "effective_date"))
    by_cust = W.partitionBy("customer_id").orderBy("effective_date")
    changed = (dedup.withColumn("prev_region", F.lag("region").over(by_cust))
                    .filter(F.col("prev_region").isNull() | (F.col("prev_region") != F.col("region"))))
    return (changed.withColumn("valid_to", F.lead("effective_date").over(by_cust))
                   .select("customer_id", "region", F.col("effective_date").alias("valid_from"), "valid_to",
                           F.col("valid_to").isNull().alias("is_current")))


def ss3_longest_streak(orders):
    F, W = _F()
    days = orders.select("customer_id", "order_date").distinct()
    w = W.partitionBy("customer_id").orderBy("order_date")
    grouped = (days.withColumn("rn", F.row_number().over(w))
                   .withColumn("grp", F.expr("date_sub(order_date, rn)")))   # constant within a run of days
    return (grouped.groupBy("customer_id", "grp").count()
                   .groupBy("customer_id").agg(F.max("count").alias("longest_streak")))


# qid -> (reference fn, input dataset names, output columns)
SPARK = {
    "JS1": (js1_revenue_by_region, ["orders", "customers"], ["region", "revenue"]),
    "JS2": (js2_customers_without_orders, ["orders", "customers"], ["customer_id"]),
    "JS3": (js3_order_size, ["orders"], ["order_id", "order_size"]),
    "MS1": (ms1_latest_status, ["raw_orders"], ["order_id", "status"]),
    "MS2": (ms2_last_3_orders, ["orders"], ["customer_id", "order_id", "rn"]),
    "MS3": (ms3_mom_growth, ["orders"], ["month", "revenue", "mom_pct"]),
    "MS4": (ms4_region_at_sale, ["sales", "dim_hist"], ["sale_id", "region"]),
    "SS1": (ss1_sessionize, ["events"], ["user_id", "session_id", "session_start", "session_end", "n_events"]),
    "SS2": (ss2_rebuild_scd2, ["change_log"], ["customer_id", "region", "valid_from", "valid_to", "is_current"]),
    "SS3": (ss3_longest_streak, ["orders"], ["customer_id", "longest_streak"]),
}
