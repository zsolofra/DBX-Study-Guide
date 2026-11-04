"""Hash maps & sets: complements, grouping, counting, prefix sums, LRU cache, hash partitioning."""
from collections import Counter, OrderedDict, defaultdict
from typing import Dict, Hashable, List


def two_sum(nums: List[int], target: int) -> List[int]:
    """Indices [i, j], i < j, with nums[i] + nums[j] == target (unsorted input); [] if none.

    PATTERN: store value -> index; look up the complement.
    COMPLEXITY: O(n) time, O(n) space (vs O(n^2) brute force).
    """
    seen: Dict[int, int] = {}
    for j, x in enumerate(nums):
        if target - x in seen:
            return [seen[target - x], j]
        seen[x] = j
    return []


def group_anagrams(words: List[str]) -> List[List[str]]:
    """Group words that are anagrams. Groups keep first-seen order; words keep input order.

    PATTERN: canonical key -> bucket. Key = sorted letters (O(k log k)) or a
    26-count tuple (O(k)).
    COMPLEXITY: O(n * k log k).
    TALK TRACK: "This is a groupBy: the key function is the 'grouping expression'."
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    for w in words:
        groups["".join(sorted(w))].append(w)
    return list(groups.values())


def top_k_frequent(nums: List[int], k: int) -> List[int]:
    """The k most frequent values, most frequent first (ties: smaller value first).

    PATTERN: count, then bucket sort by frequency -> O(n). heapq.nlargest is O(n log k).
    """
    counts = Counter(nums)
    buckets: List[List[int]] = [[] for _ in range(len(nums) + 1)]
    for val, c in counts.items():
        buckets[c].append(val)
    out: List[int] = []
    for c in range(len(buckets) - 1, 0, -1):
        for val in sorted(buckets[c]):
            out.append(val)
            if len(out) == k:
                return out
    return out


def longest_consecutive(nums: List[int]) -> int:
    """Length of the longest run of consecutive integers (unsorted input). O(n).

    PATTERN: set membership; only start counting at run starts (x-1 not in set).
    """
    s = set(nums)
    best = 0
    for x in s:
        if x - 1 not in s:
            length = 1
            while x + length in s:
                length += 1
            best = max(best, length)
    return best


def subarray_sum_equals_k(nums: List[int], k: int) -> int:
    """Count contiguous subarrays summing to k (negatives allowed).

    PATTERN: prefix sum + count of prior prefix sums. sum(i..j) = P[j] - P[i-1].
    COMPLEXITY: O(n) time, O(n) space.
    """
    counts = Counter({0: 1})
    prefix = total = 0
    for x in nums:
        prefix += x
        total += counts[prefix - k]
        counts[prefix] += 1
    return total


def first_unique_char(s: str) -> int:
    """Index of the first non-repeating character, or -1. O(n)."""
    counts = Counter(s)
    for i, ch in enumerate(s):
        if counts[ch] == 1:
            return i
    return -1


class LRUCache:
    """Least-recently-used cache with O(1) get/put.

    PATTERN: hash map + doubly linked list (OrderedDict gives you both).
    TALK TRACK: "The Delta / disk cache and Spark's block manager evict with
    LRU-style policies - recently scanned data stays hot."
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.data: "OrderedDict[Hashable, object]" = OrderedDict()

    def get(self, key: Hashable) -> object:
        if key not in self.data:
            return -1
        self.data.move_to_end(key)
        return self.data[key]

    def put(self, key: Hashable, value: object) -> None:
        if key in self.data:
            self.data.move_to_end(key)
        self.data[key] = value
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)


def hash_partition(keys: List[Hashable], num_partitions: int) -> Dict[int, List[Hashable]]:
    """Assign keys to partitions with hash(key) % n - exactly what a Spark shuffle does.

    TALK TRACK: "All rows for the same key land in the same partition, which is
    what makes a join/groupBy correct after a shuffle - and also why ONE hot key
    creates ONE giant partition (skew). Salting adds a random suffix to the key
    to spread it across partitions."
    NOTE: Python salts str hashes per process (PYTHONHASHSEED); Spark uses
    Murmur3, which is deterministic.
    """
    parts: Dict[int, List[Hashable]] = defaultdict(list)
    for k in keys:
        parts[hash(k) % num_partitions].append(k)
    return dict(parts)
