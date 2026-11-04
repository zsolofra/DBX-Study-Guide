"""Arrays: two pointers, sliding window, prefix sums, Kadane, in-place tricks."""
from collections import deque
from typing import List, Tuple


def two_sum_sorted(nums: List[int], target: int) -> Tuple[int, int]:
    """Return indices (i, j), i < j, of two numbers in a SORTED list that sum to target.
    Return (-1, -1) if none.

    PATTERN: two pointers converging from both ends.
    COMPLEXITY: O(n) time, O(1) space.
    TALK TRACK: "Because the input is sorted, moving the left pointer only ever
    increases the sum and moving the right only decreases it, so each step
    discards one candidate safely."
    """
    lo, hi = 0, len(nums) - 1
    while lo < hi:
        s = nums[lo] + nums[hi]
        if s == target:
            return lo, hi
        if s < target:
            lo += 1
        else:
            hi -= 1
    return -1, -1


def max_subarray(nums: List[int]) -> int:
    """Largest sum of a non-empty contiguous subarray.

    PATTERN: Kadane's algorithm (DP with O(1) state).
    COMPLEXITY: O(n) time, O(1) space.
    TALK TRACK: "At each index the best subarray ending here either extends the
    previous one or starts fresh - whichever is larger."
    """
    best = cur = nums[0]
    for x in nums[1:]:
        cur = max(x, cur + x)
        best = max(best, cur)
    return best


def product_except_self(nums: List[int]) -> List[int]:
    """out[i] = product of all nums except nums[i], without division.

    PATTERN: prefix / suffix products.
    COMPLEXITY: O(n) time, O(1) extra space (output excluded).
    """
    n = len(nums)
    out = [1] * n
    prefix = 1
    for i in range(n):
        out[i] = prefix
        prefix *= nums[i]
    suffix = 1
    for i in range(n - 1, -1, -1):
        out[i] *= suffix
        suffix *= nums[i]
    return out


def merge_intervals(intervals: List[List[int]]) -> List[List[int]]:
    """Merge overlapping [start, end] intervals.

    PATTERN: sort then sweep.
    COMPLEXITY: O(n log n) time (sort dominates), O(n) output.
    TALK TRACK: "Same idea as merging overlapping time ranges for SCD2 validity
    windows or sessionizing events."
    """
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda iv: iv[0])
    merged = [list(intervals[0])]
    for start, end in intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def rotate_right(nums: List[int], k: int) -> None:
    """Rotate list right by k steps IN PLACE.

    PATTERN: triple reverse.
    COMPLEXITY: O(n) time, O(1) space.
    """
    n = len(nums)
    if n == 0:
        return
    k %= n

    def rev(i: int, j: int) -> None:
        while i < j:
            nums[i], nums[j] = nums[j], nums[i]
            i, j = i + 1, j - 1

    rev(0, n - 1)
    rev(0, k - 1)
    rev(k, n - 1)


def move_zeroes(nums: List[int]) -> None:
    """Move all zeros to the end IN PLACE, keeping relative order of non-zeros.

    PATTERN: read/write pointers (stable partition).
    COMPLEXITY: O(n) time, O(1) space.
    """
    write = 0
    for read in range(len(nums)):
        if nums[read] != 0:
            nums[write], nums[read] = nums[read], nums[write]
            write += 1


def max_sliding_window(nums: List[int], k: int) -> List[int]:
    """Maximum of every window of size k.

    PATTERN: monotonic deque (indices, values decreasing).
    COMPLEXITY: O(n) time - each index pushed/popped once; O(k) space.
    TALK TRACK: "Naive is O(n*k). The deque keeps only candidates that could
    still be a future max. Spark analogue: a window function with
    ROWS BETWEEN k-1 PRECEDING AND CURRENT ROW."
    """
    dq: deque = deque()
    out = []
    for i, x in enumerate(nums):
        while dq and nums[dq[-1]] <= x:
            dq.pop()
        dq.append(i)
        if dq[0] <= i - k:
            dq.popleft()
        if i >= k - 1:
            out.append(nums[dq[0]])
    return out


def min_subarray_len(target: int, nums: List[int]) -> int:
    """Length of the shortest contiguous subarray (positive ints) with sum >= target; 0 if none.

    PATTERN: variable-size sliding window.
    COMPLEXITY: O(n) time, O(1) space.
    """
    best = float("inf")
    left = total = 0
    for right, x in enumerate(nums):
        total += x
        while total >= target:
            best = min(best, right - left + 1)
            total -= nums[left]
            left += 1
    return 0 if best == float("inf") else int(best)
