import pytest


def test_two_sum(hashmaps):
    assert hashmaps.two_sum([2, 7, 11, 15], 9) == [0, 1]
    assert hashmaps.two_sum([3, 2, 4], 6) == [1, 2]
    assert hashmaps.two_sum([3, 3], 6) == [0, 1]
    assert hashmaps.two_sum([1, 2], 10) == []


def test_group_anagrams(hashmaps):
    out = hashmaps.group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"])
    assert sorted(sorted(g) for g in out) == [["ate", "eat", "tea"], ["bat"], ["nat", "tan"]]


def test_top_k_frequent(hashmaps):
    assert hashmaps.top_k_frequent([1, 1, 1, 2, 2, 3], 2) == [1, 2]
    assert hashmaps.top_k_frequent([4, 4, 5, 5, 6], 2) == [4, 5]


@pytest.mark.parametrize("nums,expected", [
    ([100, 4, 200, 1, 3, 2], 4), ([0, 3, 7, 2, 5, 8, 4, 6, 0, 1], 9), ([], 0),
])
def test_longest_consecutive(hashmaps, nums, expected):
    assert hashmaps.longest_consecutive(nums) == expected


@pytest.mark.parametrize("nums,k,expected", [
    ([1, 1, 1], 2, 2), ([1, 2, 3], 3, 2), ([1, -1, 0], 0, 3),
])
def test_subarray_sum_equals_k(hashmaps, nums, k, expected):
    assert hashmaps.subarray_sum_equals_k(nums, k) == expected


def test_first_unique_char(hashmaps):
    assert hashmaps.first_unique_char("leetcode") == 0
    assert hashmaps.first_unique_char("loveleetcode") == 2
    assert hashmaps.first_unique_char("aabb") == -1


def test_lru_cache(hashmaps):
    c = hashmaps.LRUCache(2)
    c.put(1, 1)
    c.put(2, 2)
    assert c.get(1) == 1
    c.put(3, 3)          # evicts 2 (least recently used)
    assert c.get(2) == -1
    c.put(4, 4)          # evicts 1
    assert c.get(1) == -1
    assert c.get(3) == 3
    assert c.get(4) == 4


def test_hash_partition(hashmaps):
    keys = list(range(100)) + ["hot"] * 50
    parts = hashmaps.hash_partition(keys, 8)
    assert sum(len(v) for v in parts.values()) == len(keys)
    assert all(0 <= p < 8 for p in parts)
    hot_parts = [p for p, ks in parts.items() if "hot" in ks]
    assert len(hot_parts) == 1  # same key -> same partition (the root cause of skew)
