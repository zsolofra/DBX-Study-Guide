# DS&A + Optimization Cheat Sheet

## Pattern recognition: "if you see X, think Y"

| Signal in the prompt | Pattern | Typical complexity | Reference |
|---|---|---|---|
| Sorted array, find pair/target | Two pointers / binary search | O(n) / O(log n) | `arrays.two_sum_sorted`, `binary_search.*` |
| Unsorted, "find complement / seen before" | Hash map | O(n) time, O(n) space | `hash_maps.two_sum` |
| Contiguous subarray / substring, "longest/shortest with condition" | Sliding window | O(n) | `strings.length_of_longest_unique_substring`, `arrays.min_subarray_len` |
| Subarray sum equals k (negatives allowed) | Prefix sum + hash map | O(n) | `hash_maps.subarray_sum_equals_k` |
| Max/min over every window | Monotonic deque | O(n) | `arrays.max_sliding_window` |
| Max subarray sum | Kadane | O(n) | `arrays.max_subarray` |
| Overlapping ranges | Sort + sweep | O(n log n) | `arrays.merge_intervals` |
| "Minimum X such that it works" | Binary search on the answer | O(n log range) | `binary_search.ship_within_days` |
| Rotated sorted | Modified binary search | O(log n) | `binary_search.search_rotated` |
| Grouping by a canonical form | Hash map of key -> list | O(n k log k) | `hash_maps.group_anagrams` |
| Top-k / most frequent | Heap O(n log k) or bucket sort O(n) | | `hash_maps.top_k_frequent` |
| Matching brackets / nested structure | Stack | O(n) | `strings.valid_parentheses` |
| Shortest path, unweighted | BFS | O(V+E) | `graphs.shortest_path` |
| Shortest path, weighted (non-negative) | Dijkstra | O((V+E) log V) | `graphs.dijkstra` |
| Explore all / connected regions | DFS/BFS flood fill | O(V+E) | `graphs.num_islands` |
| Dependencies / ordering | Topological sort (Kahn) | O(V+E) | `graphs.topological_sort` |
| "Are these connected" / merging groups | Union-find | ~O(1) amortized | `graphs.UnionFind` |
| Recent-use eviction, O(1) | Hash map + doubly linked list | O(1) | `hash_maps.LRUCache` |

## Python data structure costs

| Operation | list | dict / set | deque | heapq |
|---|---|---|---|---|
| index / lookup | O(1) by index | O(1) avg by key | O(n) middle | - |
| `x in` | **O(n)** | **O(1)** avg | O(n) | O(n) |
| append / add | O(1) amortized | O(1) avg | O(1) both ends | push O(log n) |
| pop(0) / insert(0) | **O(n)** | - | **O(1)** popleft | pop min O(log n) |
| sort | O(n log n) (Timsort, stable) | - | - | heapify O(n) |

## How to answer any coding question (the 5 steps)
1. Clarify: input size, sorted?, duplicates?, negatives?, empty?, return format.
2. Brute force first + its complexity (shows you can always get *an* answer).
3. Optimize: name the pattern and the invariant that makes it correct.
4. Code cleanly, talk while you type; then dry-run a small example by hand.
5. State final time/space, edge cases, and "how would this scale on Spark?"

## Big-O intuition for data volumes
- n = 10^6 rows: O(n log n) fine in Python (~1s); O(n^2) = 10^12 ops -> never.
- n = 10^9+ rows: anything single-machine is out -> distribute; now shuffle bytes, skew and file layout dominate.

## Spark complexity translations
| Python idea | Spark equivalent |
|---|---|
| dict lookup join | Broadcast hash join |
| sort both + merge | Sort-merge join |
| `hash(k) % n` | Shuffle / hash partitioning |
| Counter / groupby | `groupBy().agg()` (partial agg -> shuffle -> final agg) |
| heap top-k | `orderBy().limit(k)` -> TakeOrderedAndProject |
| sliding window | Window functions with frames |
| memoization | Materialized views, gold tables, caching |
| binary search on sorted data | Data skipping via file min/max stats |
