"""Build notebooks/06_dsa_code_walkthrough.ipynb: one explained section per function in dsa/.

Each code cell holds the function's real source (read from dsa/, docstrings removed because the
markdown explains it), so the notebook can't drift from the code. Re-run after editing dsa/:

    python tools/build_dsa_walkthrough.py
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from py_to_ipynb import build_notebook, write_notebook  # noqa: E402

OUTPUT = ROOT / "notebooks" / "06_dsa_code_walkthrough.ipynb"

HEADER = """# DS&A code walkthrough: every function in `dsa/`
**Course:** DBX Study Guide | **Duration:** ~3 hours, or one part at a time | **Runs on:** any compute (plain Python, no Spark)

This notebook goes through the `dsa/` folder file by file. Each section gives you the **problem**, the **idea** and the **complexity**, then a code cell with the
function's real source and a small example, then **what to notice** in the output.

| Part | File | What's in it |
|---|---|---|
| 1 | `arrays.py` | two pointers, Kadane, prefix/suffix, intervals, in-place tricks, sliding windows |
| 2 | `strings.py` | two pointers, counting, sliding windows, expand around center, stacks |
| 3 | `binary_search.py` | classic search, boundaries, rotated arrays, binary search on the answer |
| 4 | `graphs.py` | BFS, DFS, grids, cycles, topological sort, Dijkstra, union-find |
| 5 | `hash_maps.py` | complements, grouping, counting, prefix sums, LRU cache, hash partitioning |
| 6 | `python_optimizations.py` | 7 slow vs fast pairs |
| 7 | `benchmark.py` | measuring Big-O from real timings |

`dsa/__init__.py` only marks the folder as a Python package, so it has nothing to walk through.

> **Spoiler warning:** these are the solutions to `exercises/`. Try a problem in `01_dsa_practice` first if you want to test yourself.

### Setup
Find the repo root and put it on the Python path. Only Part 7 imports from it, because every other cell defines its function right here."""

SETUP = '''import os
import sys

path = os.path.abspath(os.getcwd())
while not os.path.isdir(os.path.join(path, "dsa")):
    parent = os.path.dirname(path)
    if parent == path:
        raise RuntimeError("Repo root not found. Keep the folder structure from GitHub intact.")
    path = parent
if path not in sys.path:
    sys.path.insert(0, path)
print("Repo root:", path)'''

FOOTER = """# Summary
You've now read every function in `dsa/`. To lock it in:
1. Pick a topic in `01_dsa_practice` and solve its problems in `exercises/` without looking back here.
2. Use `docs/cheatsheet.md` to practice naming the pattern from the problem statement alone.
3. For each function, say the idea and complexity out loud in under 30 seconds."""

# module -> (part title and overview, [(function names, problem, idea, complexity, demo code, what to notice)])
PARTS = [
    ("arrays", "Part 1: `dsa/arrays.py`\nArray questions usually ask you to replace a nested loop with **one pass** by keeping the right state. Start by loading what this file imports.", [
        (["two_sum_sorted"], "In a **sorted** list, find the indices of two numbers that add up to `target`.",
         "Start a pointer at each end. If the sum is too small, only moving `lo` right can raise it. If it's too big, only moving `hi` left can lower it.",
         "O(n) time, O(1) space",
         'print(two_sum_sorted([1, 2, 4, 7, 11], 15))\nprint(two_sum_sorted([1, 2, 3], 100))',
         "The pointers tried 1+11=12, 2+11=13, then 4+11=15, so the answer is `(2, 4)`. Sorting is what makes each move safe. On unsorted input, use the hash map version in Part 5."),
        (["max_subarray"], "Find the largest sum of any contiguous, non-empty subarray.",
         "This is **Kadane's algorithm**. The best subarray ending at each position either extends the previous one or starts fresh, whichever is larger.",
         "O(n) time, O(1) space",
         'print(max_subarray([-2, 1, -3, 4, -1, 2, 1, -5, 4]))\nprint(max_subarray([-3, -1, -2]))',
         "The first answer is 6, from `[4, -1, 2, 1]`. The all-negative list returns -1, not 0, because the subarray can't be empty. That's why the code starts from `nums[0]` rather than 0."),
        (["product_except_self"], "Return a list where each position holds the product of every **other** element, without using division.",
         "Each answer is (product of everything to the left) x (product of everything to the right). One forward pass fills in the left products, and one backward pass multiplies in the right products.",
         "O(n) time, O(1) extra space",
         'print(product_except_self([1, 2, 3, 4]))\nprint(product_except_self([-1, 1, 0, -3, 3]))',
         "After the forward pass, the first list is `[1, 1, 2, 6]`, and the backward pass turns it into `[24, 12, 8, 6]`. Zeros need no special handling, which is a big advantage over dividing the total product."),
        (["merge_intervals"], "Merge all overlapping `[start, end]` intervals.",
         "Sort by start, then sweep. Each interval either overlaps the last merged one (extend its end) or starts a new one.",
         "O(n log n) time, from the sort",
         'print(merge_intervals([[1, 3], [8, 10], [2, 6], [15, 18]]))\nprint(merge_intervals([[1, 4], [4, 5]]))',
         "`[1, 3]` and `[2, 6]` become `[1, 6]`. Touching intervals merge too, because the check is `<=`. The same logic checks that SCD2 validity windows don't overlap."),
        (["rotate_right"], "Rotate a list right by `k` steps **in place**.",
         "Use three reversals: the whole list, then the first `k` elements, then the rest. `k %= n` handles `k` larger than the list.",
         "O(n) time, O(1) space",
         'nums = [1, 2, 3, 4, 5, 6, 7]\nrotate_right(nums, 3)\nprint(nums)',
         "The list goes `7654321`, then `5674321`, then `5671234`. The function returns nothing because it changes the list you passed in, so print the list, not the return value."),
        (["move_zeroes"], "Move all zeros to the end **in place**, keeping the other values in order.",
         "`read` visits every element, and `write` marks where the next non-zero belongs. Swapping non-zeros to `write` pushes zeros back.",
         "O(n) time, O(1) space",
         'nums = [0, 1, 0, 3, 12]\nmove_zeroes(nums)\nprint(nums)',
         "The non-zeros keep their order: `[1, 3, 12, 0, 0]`. This read/write pointer pattern also solves in-place dedupe and filtering."),
        (["max_sliding_window"], "Return the maximum of every window of size `k`.",
         "Keep a **monotonic deque** of indices with decreasing values. A new value removes smaller ones from the back, because they can never be the max again. The front is always the current max.",
         "O(n) time, because each index is added and removed at most once; O(k) space",
         'print(max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3))',
         "The result is `[3, 3, 5, 5, 6, 7]`. A naive scan of each window is O(n*k). In Spark SQL you'd write `MAX(x) OVER (ROWS BETWEEN k-1 PRECEDING AND CURRENT ROW)`."),
        (["min_subarray_len"], "Find the length of the shortest subarray of **positive** integers whose sum is at least `target`, or 0 if there isn't one.",
         "Use a variable-size window: grow on the right, and while the sum still meets the target, record the length and shrink from the left.",
         "O(n) time, O(1) space",
         'print(min_subarray_len(7, [2, 3, 1, 2, 4, 3]))\nprint(min_subarray_len(100, [1, 2, 3]))',
         "The first answer is 2, from `[4, 3]`. This only works because values are positive, so shrinking always lowers the sum. With negatives, you need prefix sums (`subarray_sum_equals_k` in Part 5)."),
    ]),
    ("strings", "Part 2: `dsa/strings.py`\nString questions reuse the array patterns, plus counting and stacks.", [
        (["is_palindrome"], "Check whether a string reads the same both ways, ignoring case and anything that isn't a letter or digit.",
         "Move two pointers inward, skipping characters that don't count.",
         "O(n) time, O(1) space",
         'print(is_palindrome("A man, a plan, a canal: Panama"))\nprint(is_palindrome("race a car"))',
         "Skipping in place avoids building a cleaned copy of the string, which is what keeps the space at O(1)."),
        (["is_anagram"], "Check whether one string is a rearrangement of another.",
         "Anagrams have the same letter counts, so compare two `Counter`s. The length check exits early.",
         "O(n) time, O(k) space for k distinct characters",
         'print(is_anagram("listen", "silent"))\nprint(is_anagram("aacc", "ccac"))',
         "`aacc` and `ccac` use the same letters with different counts, so comparing sets would wrongly say they match. Sorting also works, but it's O(n log n)."),
        (["length_of_longest_unique_substring"], "Find the length of the longest substring with no repeated characters.",
         "Slide a window and remember each character's last index. On a repeat inside the window, jump `left` past the earlier copy.",
         "O(n) time, O(min(n, alphabet)) space",
         'print(length_of_longest_unique_substring("pwwkew"))\nprint(length_of_longest_unique_substring("abba"))',
         "`abba` returns 2. The `>= left` check matters here: the final `a`'s earlier copy is already outside the window, so `left` must not move back."),
        (["longest_palindromic_substring"], "Find the longest palindromic substring.",
         "Every palindrome grows out from a center. Try all 2n-1 centers: each character for odd lengths, and each gap for even lengths.",
         "O(n^2) time, O(1) space",
         'print(longest_palindromic_substring("babad"))\nprint(longest_palindromic_substring("cbbd"))',
         "`cbbd` returns `bb`, which only the even-length center finds. Manacher's algorithm is O(n): mention it, but you're rarely asked to code it."),
        (["min_window_substring"], "Find the smallest substring of `s` that contains every character of `t`, including duplicates.",
         "`need` counts what's still owed, and `missing` counts how many characters are outstanding. Grow right until `missing` is 0, then shrink left past anything extra.",
         "O(|s| + |t|) time",
         'print(min_window_substring("ADOBECODEBANC", "ABC"))\nprint(min_window_substring("a", "aa"))',
         "The answer is `BANC`. `need` can go negative, which marks a surplus character that the window can safely drop. This is a common hard interview question, so trace it by hand once."),
        (["compress"], "Run-length encode a string (`aaabcc` becomes `a3b1c2`), returning the original if encoding isn't shorter.",
         "Make one pass, tracking the current character and its run length. Collect the pieces in a list and join them at the end.",
         "O(n) time",
         'print(compress("aaabccccc"))\nprint(compress("abc"))',
         "`abc` stays as it is, because `a1b1c1` is longer. Parquet uses this same idea, run-length encoding, which is why clustered data compresses well."),
        (["reverse_words"], "Reverse the word order and normalize the spaces.",
         "`split()` with no argument handles repeated and leading spaces. Reverse the words, then join them with single spaces.",
         "O(n) time",
         'print(reverse_words("  the sky  is blue "))',
         "A one-liner is fine here. If the interviewer asks for O(1) space on a mutable array, reverse the whole thing, then each word: the same trick as `rotate_right`."),
        (["valid_parentheses"], "Check that every bracket closes in the correct order.",
         "Use a **stack**. Push openers. Each closer must match the most recent opener.",
         "O(n) time, O(n) space",
         'print(valid_parentheses("([{}])"))\nprint(valid_parentheses("(]"))\nprint(valid_parentheses("("))',
         "The final `return not stack` catches `(`, which has no mismatch but never closes. Use a stack whenever the most recent item matters, as with nesting."),
    ]),
    ("binary_search", "Part 3: `dsa/binary_search.py`\nBinary search applies to any problem where you can safely discard half the candidates, not just sorted lookups.", [
        (["binary_search"], "Find the index of `target` in a sorted list, or return -1.",
         "Check the middle and discard the half that can't contain the target. The invariant: if the target exists, it's inside `nums[lo..hi]`.",
         "O(log n) time, O(1) space",
         'print(binary_search([-1, 0, 3, 5, 9, 12], 9))\nprint(binary_search([-1, 0, 3, 5, 9, 12], 2))',
         "`lo <= hi` and `mid +/- 1` go together. With an inclusive `hi`, a one-element range still needs checking, and `mid` has already been ruled out."),
        (["lower_bound"], "Find the first index whose value is `>= target`. It's the same as `bisect.bisect_left`.",
         "Use a half-open range `[lo, hi)`. If `nums[mid]` is too small, the answer is to the right. Otherwise `mid` might be the answer, so keep it with `hi = mid`.",
         "O(log n) time",
         'print(lower_bound([1, 3, 3, 5], 3))\nprint(lower_bound([1, 3, 3, 5], 4))\nprint(lower_bound([1, 3, 3, 5], 9))',
         "It finds the **first** 3 (index 1), and if the target isn't there it returns where it would go. Note the different template: `lo < hi` and `hi = mid`."),
        (["search_range"], "Find the first and last index of `target` in a sorted list.",
         "Run two boundary searches: the first position of `target`, and the first position of `target + 1`, minus one.",
         "O(log n) time",
         'print(search_range([5, 7, 7, 8, 8, 10], 8))\nprint(search_range([5, 7, 7, 8, 8, 10], 6))',
         "This stays O(log n) however many duplicates there are. Scanning outward from a match would be O(n) in the worst case. It reuses `lower_bound` from the previous section."),
        (["search_rotated"], "Search a sorted list that has been rotated, with distinct values.",
         "At least one half is always sorted. Find which half, then check whether the target falls inside its range.",
         "O(log n) time",
         'print(search_rotated([4, 5, 6, 7, 0, 1, 2], 0))\nprint(search_rotated([4, 5, 6, 7, 0, 1, 2], 3))',
         "The first answer is index 4. Comparing `nums[lo] <= nums[mid]` tells you the left half is sorted. This is one of the most common binary search interview questions."),
        (["find_min_rotated"], "Find the minimum of a rotated sorted list with distinct values.",
         "Compare the middle with the **right** end. If `nums[mid] > nums[hi]`, the drop is to the right of `mid`. Otherwise the minimum is at `mid` or to its left.",
         "O(log n) time",
         'print(find_min_rotated([3, 4, 5, 1, 2]))\nprint(find_min_rotated([11, 13, 15, 17]))',
         "It also handles a list that was never rotated. The loop ends with `lo == hi` on the minimum, using the same `lo < hi` template as `lower_bound`."),
        (["int_sqrt"], "Compute `floor(sqrt(x))` without `math.sqrt`.",
         "Binary search over possible answers, using `mid * mid <= x` as the test.",
         "O(log x) time",
         'print(int_sqrt(10))\nprint(int_sqrt(16))\nprint(int_sqrt(0))',
         "When the loop ends, `hi` is the largest value that passed, which is why the function returns `hi` rather than `lo`."),
        (["search_matrix"], "Search a matrix where rows are sorted and each row starts after the previous row ends.",
         "Treat it as one flat sorted list. Index `mid` maps to row `mid // n` and column `mid % n`.",
         "O(log(m*n)) time",
         'matrix = [[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]]\nprint(search_matrix(matrix, 3))\nprint(search_matrix(matrix, 13))',
         "No special 2D logic is needed once you see the mapping. That index conversion is the whole trick."),
        (["ship_within_days"], "Find the smallest ship capacity that delivers packages, in order, within `days`.",
         "This is **binary search on the answer**. If a capacity works, every larger one works too, so search for the first that passes. The range runs from the heaviest package to the total weight.",
         "O(n log(sum(weights))) time",
         'print(ship_within_days([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5))\nprint(ship_within_days([3, 2, 2, 4, 1, 4], 3))',
         "The first answer is 15. Look for the phrase \"minimum X such that...\". \"Smallest cluster that meets the SLA\" has the same shape."),
        (["find_peak_element"], "Find the index of any element larger than both neighbors.",
         "If `nums[mid] < nums[mid + 1]`, the values rise to the right, so a peak must exist there. Otherwise a peak is at `mid` or to its left.",
         "O(log n) time",
         'print(find_peak_element([1, 2, 1, 3, 5, 6, 4]))',
         "It returns index 5 (value 6), even though index 1 is also a peak: any peak is acceptable. This shows binary search doesn't need fully sorted data, only a way to discard a half."),
    ]),
    ("graphs", "Part 4: `dsa/graphs.py`\nGraphs are stored as adjacency lists: a dict from each node to its neighbors. Job dependencies, lineage and entity resolution are all graph problems.", [
        (["build_graph"], "Turn an edge list into an adjacency list.",
         "Add each edge once if it's directed, or in both directions if not. For directed graphs, `setdefault` makes sure nodes with no outgoing edges still appear.",
         "O(V + E) time and space",
         'edges = [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "E")]\ngraph = build_graph(edges)\nprint(graph)\nprint(build_graph([(1, 2)], directed=True))',
         "In the directed example, node 2 appears with an empty list. Without that, code that looks up `graph[2]` would fail. `graph` is used in the next few sections."),
        (["bfs_order"], "List nodes in breadth-first order.",
         "Use a queue and a visited set, and mark nodes visited **when you add them**, so none is queued twice.",
         "O(V + E) time, O(V) space",
         'print(bfs_order(graph, "A"))',
         "BFS visits in rings: A, then its neighbors B and C, then D, then E."),
        (["dfs_order"], "List nodes in depth-first order, without recursion.",
         "Use an explicit stack, and push neighbors in reverse so they're visited in list order.",
         "O(V + E) time",
         'print(dfs_order(graph, "A"))',
         "DFS goes deep first: A, B, D, then C and E. The explicit stack avoids Python's recursion limit of about 1,000 on deep graphs, and here the visited check happens when a node is popped."),
        (["shortest_path"], "Find a path with the fewest edges in an unweighted graph.",
         "BFS reaches each node first by a shortest path. Record each node's parent, then walk back from the goal.",
         "O(V + E) time",
         'print(shortest_path(graph, "A", "E"))\nprint(shortest_path(graph, "A", "Z"))',
         "The result is built backward from the goal, then reversed with `[::-1]`. `parent` also acts as the visited set. Use Dijkstra if edges have different costs."),
        (["num_islands"], "Count connected groups of `\"1\"` in a grid.",
         "A grid is a graph where each cell's neighbors are its 4 adjacent cells. Each unvisited land cell starts a new island, and a flood fill marks the rest of it.",
         "O(rows * cols) time",
         'grid = [list("11000"), list("11000"), list("00100"), list("00011")]\nprint(num_islands(grid))',
         "There are 3 islands. The `0 <= nr < rows` bounds check comes before reading `grid[nr][nc]`, which avoids index errors and wraparound from negative indexes."),
        (["has_cycle_directed"], "Detect a cycle in a directed graph.",
         "Use DFS with three colors: WHITE is unvisited, GRAY is on the current path, BLACK is finished. An edge to a GRAY node goes back into the current path, so it's a cycle.",
         "O(V + E) time",
         'print(has_cycle_directed({1: [2], 2: [3], 3: [1]}))\nprint(has_cycle_directed({1: [2, 3], 2: [3], 3: []}))',
         "In the second graph, node 3 is reached twice without a cycle. A plain visited set would wrongly flag this, which is why GRAY and BLACK are separate."),
        (["topological_sort"], "Order tasks so every dependency comes first, or return `None` if there's a cycle.",
         "This is **Kahn's algorithm**: repeatedly take a node with in-degree 0 and reduce its children's in-degrees. A heap makes the tie-breaking deterministic.",
         "O(V log V + E) with the heap, O(V + E) with a plain queue",
         'dag = {"raw": ["stg"], "stg": ["dim", "fact"], "dim": ["fact"], "fact": ["gold"], "gold": []}\nprint(topological_sort(dag))\nprint(topological_sort({"a": ["b"], "b": ["a"]}))',
         "`fact` waits for both `stg` and `dim`. If some nodes never reach in-degree 0, there's a cycle, so the code returns `None`. Job schedulers order task DAGs the same way."),
        (["dijkstra"], "Find the cheapest distance from `start` to every reachable node, with non-negative weights.",
         "Use a min-heap of `(distance, node)`. Instead of updating entries, push new ones and skip stale ones when they pop.",
         "O((V + E) log V) time",
         'weighted = {"A": [("B", 4), ("C", 1)], "C": [("B", 2), ("D", 5)], "B": [("D", 1)], "D": []}\nprint(dijkstra(weighted, "A"))',
         "B costs 3 through C, not 4 directly, and D costs 4 through C and B. Negative weights break it; use Bellman-Ford for those."),
        (["UnionFind"], "Track groups that merge over time, and check whether two items are in the same group.",
         "Each item points to a parent, and the root names the group. `find` compresses paths as it walks, and `union` attaches the smaller group to the larger.",
         "Nearly O(1) per operation (amortized)",
         'uf = UnionFind()\nuf.union("email:a@x.com", "phone:555")\nuf.union("phone:555", "crm:42")\nprint(uf.find("email:a@x.com") == uf.find("crm:42"))\nprint(uf.find("email:a@x.com") == uf.find("crm:99"))',
         "Email and CRM records link through the shared phone number even though they never met directly. This is entity resolution: building one customer from several source systems."),
        (["count_components"], "Count connected groups among nodes `0..n-1`.",
         "Start with `n` groups. Each `union` that joins two different groups reduces the count by one.",
         "Nearly O(n + edges)",
         'print(count_components(5, [(0, 1), (1, 2), (3, 4)]))\nprint(count_components(3, []))',
         "`union` returns `False` when both items are already together, so counting the `True` results gives the number of real merges. It reuses `UnionFind` from the previous section."),
    ]),
    ("hash_maps", "Part 5: `dsa/hash_maps.py`\nHash maps trade memory for O(1) average lookups. They also underlie Spark's hash joins, hash aggregation and shuffles.", [
        (["two_sum"], "Find indices of two numbers that add up to `target`, in an **unsorted** list.",
         "For each value, check whether its complement has already been seen, then store the value and its index.",
         "O(n) time, O(n) space (brute force is O(n^2))",
         'print(two_sum([2, 7, 11, 15], 9))\nprint(two_sum([3, 3], 6))',
         "Storing **after** checking means a value can't pair with itself, yet `[3, 3]` still works because the second 3 finds the first."),
        (["group_anagrams"], "Group words that are anagrams of each other.",
         "Give every anagram the same key (its sorted letters), and put each word in that key's bucket.",
         "O(n * k log k) time for n words of length k",
         'print(group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"]))',
         "Choosing a canonical key is the whole idea, and it's exactly what `groupBy` does with a grouping expression. A 26-letter count tuple as the key would make each word O(k)."),
        (["top_k_frequent"], "Return the `k` most frequent values, breaking ties with the smaller value.",
         "Count the values, then put them into buckets indexed by frequency. Read the buckets from highest to lowest.",
         "O(n) time, plus sorting within each bucket for the ties",
         'print(top_k_frequent([1, 1, 1, 2, 2, 3], 2))\nprint(top_k_frequent([4, 4, 5, 5, 6], 2))',
         "A value can't appear more than n times, so there are only n+1 buckets. That bound is what avoids a full sort. `heapq.nlargest` is the simpler O(n log k) alternative."),
        (["longest_consecutive"], "Find the length of the longest run of consecutive integers in an unsorted list.",
         "Put everything in a set. Only start counting at a run's beginning, where `x - 1` isn't in the set.",
         "O(n) time",
         'print(longest_consecutive([100, 4, 200, 1, 3, 2]))\nprint(longest_consecutive([]))',
         "It looks like a nested loop, but each run is walked only once, from its start, so the total is O(n). In SQL the same problem is solved with gaps and islands."),
        (["subarray_sum_equals_k"], "Count contiguous subarrays summing to `k`, with negatives allowed.",
         "A subarray's sum is the difference of two prefix sums. For each prefix, count how many earlier prefixes equal `prefix - k`.",
         "O(n) time, O(n) space",
         'print(subarray_sum_equals_k([1, 1, 1], 2))\nprint(subarray_sum_equals_k([1, -1, 0], 0))',
         "Starting the counter with `{0: 1}` covers subarrays that begin at index 0. A sliding window can't do this, because negatives mean growing the window doesn't always raise the sum."),
        (["first_unique_char"], "Find the index of the first character that appears exactly once, or return -1.",
         "Make two passes: count every character, then find the first with a count of 1.",
         "O(n) time",
         'print(first_unique_char("loveleetcode"))\nprint(first_unique_char("aabb"))',
         "The second pass goes over the **string**, not the counter, because you need the first position in the original order."),
        (["LRUCache"], "Build a cache that evicts the least recently used key, with O(1) `get` and `put`.",
         "An `OrderedDict` combines a hash map with a linked list. Move a key to the end on every use, and evict from the front.",
         "O(1) per operation",
         'cache = LRUCache(2)\ncache.put(1, "a")\ncache.put(2, "b")\nprint(cache.get(1))\ncache.put(3, "c")\nprint(cache.get(2))\nprint(list(cache.data))',
         "`get(1)` made key 1 recent, so adding key 3 evicted key 2. Moving the key on `put` matters too: without it, an updated key keeps its old position. `30_senior_walkthrough` builds this from scratch."),
        (["hash_partition"], "Assign keys to partitions using `hash(key) % n`, which is what a Spark shuffle does.",
         "The same key always lands in the same partition. That's what makes joins and aggregations correct after a shuffle.",
         "O(n) time",
         'parts = hash_partition(list(range(100)) + ["hot"] * 50, 8)\nsizes = sorted((len(v) for v in parts.values()), reverse=True)\nprint("rows per partition:", sizes)',
         "All 50 `hot` rows land in one partition, which is **data skew**. String hashes in Python change between runs, so which partition gets them varies, and so do the sizes. Spark uses Murmur3, which is deterministic."),
    ]),
    ("python_optimizations", "Part 6: `dsa/python_optimizations.py`\nEach pair returns the same result, so you can compare only the cost. `compare_pair` below is a helper for this section: it checks that the results match, then times both.\n\nThe file also holds `PAIRS`, a dict of these pairs that `benchmark.compare` uses in Part 7.", [
        (["common_items_slow", "common_items_fast"], "Find the items of `a` that are also in `b`.",
         "`x in list` scans the list, but `x in set` is a hash lookup. Build the set once, outside the loop.",
         "O(a * b) vs O(a + b)",
         'a = list(range(3000))\nb = list(range(1500, 4500))\ncompare_pair(common_items_slow, common_items_fast, a, b)',
         "This is the most common Python performance bug: `in` on a list inside a loop. It's also the reason hash joins beat nested-loop joins."),
        (["build_csv_slow", "build_csv_fast"], "Build a large string from many rows.",
         "`+=` can copy the whole string on each step, while `\"\".join` allocates once.",
         "Up to O(n^2) vs O(n)",
         'rows = [(i, i * 2, "x") for i in range(3000)]\ncompare_pair(build_csv_slow, build_csv_fast, rows)',
         "Expect a **small** gap. CPython can often grow a string in place when nothing else references it, but that's an implementation detail, so `join` is the portable answer. Mentioning this is good interview nuance."),
        (["count_words_slow", "count_words_fast"], "Count how often each word appears.",
         "`Counter` runs its counting loop in C, while the slow version does extra lookups in Python.",
         "O(n) for both, with a smaller constant for `Counter`",
         'words = [f"w{i % 100}" for i in range(50000)]\ncompare_pair(count_words_slow, count_words_fast, words)',
         "Both are O(n), so this is a constant-factor win. Big-O isn't the whole story: built-ins are usually faster and shorter."),
        (["top_k_slow", "top_k_fast"], "Get the `k` largest values.",
         "Sorting orders everything, even though you only need `k` values. A heap tracks just those.",
         "O(n log n) vs O(n log k)",
         'import random\n\nrng = random.Random(1)\nshuffled = [rng.randint(0, 10**9) for _ in range(100000)]\nprint("Shuffled data:")\ncompare_pair(top_k_slow, top_k_fast, shuffled, 10)\nprint("Data already in ascending order:")\ncompare_pair(top_k_slow, top_k_fast, list(range(100000)), 10)',
         "On shuffled data the heap wins, and its lead grows with n. On ascending data the result **flips**: Python's sort takes advantage of existing order and is close to O(n), while the heap has to replace its contents on every element. Big-O describes growth, so real data can still change the winner. Spark's `orderBy().limit(k)` also keeps a top-k per partition."),
        (["rank_lookups_slow", "rank_lookups_fast"], "For each query, count how many sorted values are smaller.",
         "Sorted data lets `bisect` answer each query with a binary search instead of a scan.",
         "O(q * n) vs O(q log n)",
         'sorted_vals = list(range(0, 20000, 2))\nqueries = list(range(0, 20000, 100))\ncompare_pair(rank_lookups_slow, rank_lookups_fast, sorted_vals, queries)',
         "Paying once to sort makes every later lookup cheap. Delta's file statistics apply a similar idea: a range check rules out whole files."),
        (["fib_slow", "fib_fast"], "Compute Fibonacci numbers.",
         "Naive recursion recomputes the same values repeatedly. `lru_cache` stores each result, so each value is computed once.",
         "O(2^n) vs O(n)",
         'fib_fast.cache_clear()\ncompare_pair(fib_slow, fib_fast, 25)',
         "This is **memoization**. The warehouse version of the same trade-off is a materialized view: store a result once rather than recomputing it."),
        (["join_slow", "join_fast"], "Join orders `(order_id, cust_id)` to customers `(cust_id, name)`.",
         "A nested loop compares every pair. A hash join builds a dict from the small side, then probes it once per order.",
         "O(n * m) vs O(n + m)",
         'orders = [(i, i % 500) for i in range(3000)]\ncustomers = [(c, f"c{c}") for c in range(500)]\ncompare_pair(join_slow, join_fast, orders, customers)',
         "This is a **broadcast hash join** in miniature: the dict is the broadcast table. It's a strong answer to \"how does a join work?\""),
    ]),
    ("benchmark", "Part 7: `dsa/benchmark.py`\nThese tools measure complexity from real timings instead of just claiming it. `02_python_complexity_lab` uses them on bigger experiments.", [
        (["time_call"], "Time a function call reliably.",
         "Run it several times and keep the **best** time. Noise from garbage collection or other processes only ever makes a run slower.",
         "O(repeat) runs of the function",
         'seconds = time_call(sum, range(100000))\nprint(f"{seconds:.6f}s")',
         "Taking the minimum rather than the average gives the most stable estimate of the code's actual cost."),
        (["growth"], "Measure how the runtime changes as the input grows.",
         "Call `time_call` at each size and return `(n, seconds)` pairs.",
         "Depends on the function being measured",
         'rows = growth(lambda n: sum(range(n)), [10000, 100000, 1000000])\nfor n, t in rows:\n    print(f"n={n:>8,}  {t:.6f}s")',
         "Each input here is 10x bigger, so a linear function should take about 10x longer. The exact numbers change from run to run."),
        (["estimate_exponent"], "Estimate the `k` in O(n^k) from timing data.",
         "If `time = c * n^k`, then `log(time) = log(c) + k * log(n)`, a straight line whose slope is `k`. Least squares finds the slope.",
         "O(number of rows)",
         'quadratic = [(1000, 1.0), (2000, 4.0), (4000, 16.0)]\nprint(round(estimate_exponent(quadratic), 2))\nprint(round(estimate_exponent(growth(lambda n: sum(range(n)), [100000, 200000, 400000, 800000])), 2))',
         "The made-up quadratic data gives exactly 2.0, and the real `sum` gives roughly 1. As a guide, about 0 is constant, 1 is linear and 2 is quadratic. O(n log n) shows up slightly above 1."),
        (["_inputs"], "Build the sample inputs for every slow vs fast pair.",
         "It returns a dict from each pair's name to its arguments. A fixed random seed makes the data the same on every run.",
         "O(n) to build the data",
         'for name, args in _inputs(100).items():\n    print(f"{name:<32} {len(args)} argument(s)")',
         "The leading underscore marks it as an internal helper, since only `compare` calls it."),
        (["compare"], "Run all 7 pairs from Part 6 at one input size and report the speedup.",
         "For each pair, confirm that both versions return the same result, then time each one.",
         "Dominated by the slowest pair",
         'for row in compare(n=1000):\n    print(row)',
         "The `assert` matters: a speedup means nothing if the fast version returns a different answer. `cache_clear()` resets `fib_fast` so its cached results don't make it look faster than it is."),
    ]),
]

SECTION_HELPER = {
    "python_optimizations": '''

def compare_pair(slow, fast, *args):
    t0 = time.perf_counter()
    slow_result = slow(*args)
    t_slow = time.perf_counter() - t0
    t0 = time.perf_counter()
    fast_result = fast(*args)
    t_fast = time.perf_counter() - t0
    assert slow_result == fast_result
    print(f"slow: {t_slow * 1000:9.3f} ms\\nfast: {t_fast * 1000:9.3f} ms\\nspeedup: {t_slow / max(t_fast, 1e-9):,.1f}x")''',
}
EXTRA_IMPORTS = {"python_optimizations": "import time\n"}


def module_parts(module):
    text = (ROOT / "dsa" / f"{module}.py").read_text(encoding="utf-8")
    lines = text.splitlines()
    tree = ast.parse(text)

    def source_of(node):
        start = node.decorator_list[0].lineno if node.decorator_list else node.lineno
        skip = set()
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            skip = set(range(first.lineno, first.end_lineno + 1))
        return "\n".join(lines[i - 1] for i in range(start, node.end_lineno + 1) if i not in skip)

    defs = {n.name: source_of(n) for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    preamble = []
    for n in tree.body:
        is_import = isinstance(n, (ast.Import, ast.ImportFrom))
        is_alias = isinstance(n, (ast.Assign, ast.AnnAssign)) and module == "graphs"
        if is_import or is_alias:
            preamble.append("\n".join(lines[n.lineno - 1:n.end_lineno]))
    return "\n".join(preamble), defs


def build_cells():
    cells = [("markdown", HEADER), ("code", SETUP)]
    pending = None
    for module, overview, sections in PARTS:
        preamble, defs = module_parts(module)
        covered = {name for names, *_ in sections for name in names}
        if covered != set(defs):
            raise SystemExit(f"{module}: walkthrough covers {sorted(covered)} but the file has {sorted(defs)}")
        part = "# " + overview
        cells.append(("markdown", (pending + "\n\n---\n\n" if pending else "**Setup complete.**\n\n---\n\n") + part))
        cells.append(("code", EXTRA_IMPORTS.get(module, "") + preamble + SECTION_HELPER.get(module, "")))
        pending = None
        for names, problem, idea, complexity, demo, notice in sections:
            title = " and ".join(f"`{n}`" for n in names)
            intro = f"## {title}\n- **Problem:** {problem}\n- **Idea:** {idea}\n- **Complexity:** {complexity}"
            cells.append(("markdown", (pending + "\n\n---\n\n" if pending else "The imports are loaded.\n\n---\n\n") + intro))
            source = "\n\n\n".join(defs[n] for n in names)
            cells.append(("code", f"{source}\n\n\n# Try it\n{demo}"))
            pending = f"**What to notice:** {notice}"
    cells.append(("markdown", pending + "\n\n---\n\n" + FOOTER))
    return cells


def build_notebook_dict():
    return build_notebook([(k, t.split("\n")) for k, t in build_cells()], OUTPUT.stem)


if __name__ == "__main__":
    write_notebook(build_notebook_dict(), OUTPUT)
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
