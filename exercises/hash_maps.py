# AUTO-GENERATED EXERCISE STUBS - implement each function, then run the tests.
"""Hash maps & sets: complements, grouping, counting, prefix sums, LRU cache, hash partitioning."""
from collections import Counter, OrderedDict, defaultdict
from typing import Dict, Hashable, List

def two_sum(nums: List[int], target: int) -> List[int]:
    """Indices [i, j], i < j, with nums[i] + nums[j] == target (unsorted input); [] if none.

    COMPLEXITY: O(n) time, O(n) space (vs O(n^2) brute force).
    """
    raise NotImplementedError('TODO: implement me')

def group_anagrams(words: List[str]) -> List[List[str]]:
    """Group words that are anagrams. Groups keep first-seen order; words keep input order.

    COMPLEXITY: O(n * k log k).
    """
    raise NotImplementedError('TODO: implement me')

def top_k_frequent(nums: List[int], k: int) -> List[int]:
    """The k most frequent values, most frequent first (ties: smaller value first).
    """
    raise NotImplementedError('TODO: implement me')

def longest_consecutive(nums: List[int]) -> int:
    """Length of the longest run of consecutive integers (unsorted input). O(n).
    """
    raise NotImplementedError('TODO: implement me')

def subarray_sum_equals_k(nums: List[int], k: int) -> int:
    """Count contiguous subarrays summing to k (negatives allowed).

    COMPLEXITY: O(n) time, O(n) space.
    """
    raise NotImplementedError('TODO: implement me')

def first_unique_char(s: str) -> int:
    """Index of the first non-repeating character, or -1. O(n).
    """
    raise NotImplementedError('TODO: implement me')

class LRUCache:
    """Least-recently-used cache with O(1) get/put.

    PATTERN: hash map + doubly linked list (OrderedDict gives you both).
    TALK TRACK: "The Delta / disk cache and Spark's block manager evict with
    LRU-style policies - recently scanned data stays hot."
    """

    def __init__(self, capacity: int) -> None:
        raise NotImplementedError('TODO: implement me')

    def get(self, key: Hashable) -> object:
        raise NotImplementedError('TODO: implement me')

    def put(self, key: Hashable, value: object) -> None:
        raise NotImplementedError('TODO: implement me')

def hash_partition(keys: List[Hashable], num_partitions: int) -> Dict[int, List[Hashable]]:
    """Assign keys to partitions with hash(key) % n - exactly what a Spark shuffle does.

    NOTE: Python salts str hashes per process (PYTHONHASHSEED); Spark uses
    Murmur3, which is deterministic.
    """
    raise NotImplementedError('TODO: implement me')
