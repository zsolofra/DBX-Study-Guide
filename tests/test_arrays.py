import pytest


@pytest.mark.parametrize("nums,target,expected", [
    ([1, 2, 4, 7, 11], 15, (2, 4)),
    ([2, 7, 11, 15], 9, (0, 1)),
    ([1, 2, 3], 100, (-1, -1)),
    ([], 1, (-1, -1)),
])
def test_two_sum_sorted(arrays, nums, target, expected):
    assert tuple(arrays.two_sum_sorted(nums, target)) == expected


@pytest.mark.parametrize("nums,expected", [
    ([-2, 1, -3, 4, -1, 2, 1, -5, 4], 6),
    ([1], 1),
    ([-3, -1, -2], -1),
    ([5, 4, -1, 7, 8], 23),
])
def test_max_subarray(arrays, nums, expected):
    assert arrays.max_subarray(nums) == expected


def test_product_except_self(arrays):
    assert arrays.product_except_self([1, 2, 3, 4]) == [24, 12, 8, 6]
    assert arrays.product_except_self([-1, 1, 0, -3, 3]) == [0, 0, 9, 0, 0]


def test_merge_intervals(arrays):
    assert arrays.merge_intervals([[1, 3], [8, 10], [2, 6], [15, 18]]) == [[1, 6], [8, 10], [15, 18]]
    assert arrays.merge_intervals([[1, 4], [4, 5]]) == [[1, 5]]
    assert arrays.merge_intervals([]) == []


@pytest.mark.parametrize("nums,k,expected", [
    ([1, 2, 3, 4, 5, 6, 7], 3, [5, 6, 7, 1, 2, 3, 4]),
    ([1, 2], 5, [2, 1]),
    ([], 3, []),
])
def test_rotate_right(arrays, nums, k, expected):
    arrays.rotate_right(nums, k)
    assert nums == expected


def test_move_zeroes(arrays):
    nums = [0, 1, 0, 3, 12]
    arrays.move_zeroes(nums)
    assert nums == [1, 3, 12, 0, 0]


def test_max_sliding_window(arrays):
    assert arrays.max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3) == [3, 3, 5, 5, 6, 7]
    assert arrays.max_sliding_window([4], 1) == [4]


def test_min_subarray_len(arrays):
    assert arrays.min_subarray_len(7, [2, 3, 1, 2, 4, 3]) == 2
    assert arrays.min_subarray_len(100, [1, 2, 3]) == 0
