import bisect
import math
import random

import pytest


def test_binary_search(bsearch):
    nums = [-1, 0, 3, 5, 9, 12]
    assert bsearch.binary_search(nums, 9) == 4
    assert bsearch.binary_search(nums, 2) == -1
    assert bsearch.binary_search([], 1) == -1


def test_lower_bound_matches_bisect(bsearch):
    rng = random.Random(7)
    for _ in range(200):
        nums = sorted(rng.randint(0, 20) for _ in range(rng.randint(0, 15)))
        t = rng.randint(-2, 22)
        assert bsearch.lower_bound(nums, t) == bisect.bisect_left(nums, t)


def test_search_range(bsearch):
    assert bsearch.search_range([5, 7, 7, 8, 8, 10], 8) == [3, 4]
    assert bsearch.search_range([5, 7, 7, 8, 8, 10], 6) == [-1, -1]
    assert bsearch.search_range([], 0) == [-1, -1]


@pytest.mark.parametrize("nums,target,expected", [
    ([4, 5, 6, 7, 0, 1, 2], 0, 4), ([4, 5, 6, 7, 0, 1, 2], 3, -1), ([1], 1, 0), ([3, 1], 1, 1),
])
def test_search_rotated(bsearch, nums, target, expected):
    assert bsearch.search_rotated(nums, target) == expected


def test_find_min_rotated(bsearch):
    assert bsearch.find_min_rotated([3, 4, 5, 1, 2]) == 1
    assert bsearch.find_min_rotated([11, 13, 15, 17]) == 11


def test_int_sqrt(bsearch):
    for x in list(range(50)) + [2 ** 31 - 1, 10 ** 12]:
        assert bsearch.int_sqrt(x) == math.isqrt(x)


def test_search_matrix(bsearch):
    m = [[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]]
    assert bsearch.search_matrix(m, 3)
    assert not bsearch.search_matrix(m, 13)
    assert not bsearch.search_matrix([], 1)


def test_ship_within_days(bsearch):
    assert bsearch.ship_within_days([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5) == 15
    assert bsearch.ship_within_days([3, 2, 2, 4, 1, 4], 3) == 6


def test_find_peak_element(bsearch):
    nums = [1, 2, 1, 3, 5, 6, 4]
    i = bsearch.find_peak_element(nums)
    left = nums[i - 1] if i > 0 else -math.inf
    right = nums[i + 1] if i < len(nums) - 1 else -math.inf
    assert nums[i] > left and nums[i] > right
