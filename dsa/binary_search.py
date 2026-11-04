"""Binary search: classic, boundaries, rotated arrays, and 'binary search on the answer'."""
from typing import List


def binary_search(nums: List[int], target: int) -> int:
    """Index of target in sorted nums, or -1.

    COMPLEXITY: O(log n) time, O(1) space.
    INVARIANT: if target exists, it is within nums[lo..hi].
    """
    lo, hi = 0, len(nums) - 1
    while lo <= hi:
        mid = lo + (hi - lo) // 2  # overflow-safe habit from other languages
        if nums[mid] == target:
            return mid
        if nums[mid] < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return -1


def lower_bound(nums: List[int], target: int) -> int:
    """First index i with nums[i] >= target (len(nums) if none). Same as bisect.bisect_left.

    PATTERN: half-open interval [lo, hi).
    """
    lo, hi = 0, len(nums)
    while lo < hi:
        mid = (lo + hi) // 2
        if nums[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def search_range(nums: List[int], target: int) -> List[int]:
    """[first, last] index of target in sorted nums, or [-1, -1].

    TALK TRACK: "Two boundary searches = O(log n). This is how min/max column
    stats let Delta skip files: each file's [min, max] is a range check."
    """
    first = lower_bound(nums, target)
    if first == len(nums) or nums[first] != target:
        return [-1, -1]
    last = lower_bound(nums, target + 1) - 1
    return [first, last]


def search_rotated(nums: List[int], target: int) -> int:
    """Search a rotated sorted array of distinct ints. Return index or -1.

    PATTERN: one half is always sorted; decide which half could contain target.
    COMPLEXITY: O(log n).
    """
    lo, hi = 0, len(nums) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if nums[mid] == target:
            return mid
        if nums[lo] <= nums[mid]:  # left half sorted
            if nums[lo] <= target < nums[mid]:
                hi = mid - 1
            else:
                lo = mid + 1
        else:  # right half sorted
            if nums[mid] < target <= nums[hi]:
                lo = mid + 1
            else:
                hi = mid - 1
    return -1


def find_min_rotated(nums: List[int]) -> int:
    """Minimum of a rotated sorted array of distinct ints. O(log n)."""
    lo, hi = 0, len(nums) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if nums[mid] > nums[hi]:
            lo = mid + 1
        else:
            hi = mid
    return nums[lo]


def int_sqrt(x: int) -> int:
    """floor(sqrt(x)) for x >= 0 without math.sqrt. O(log x)."""
    lo, hi = 0, x
    while lo <= hi:
        mid = (lo + hi) // 2
        if mid * mid <= x:
            lo = mid + 1
        else:
            hi = mid - 1
    return hi


def search_matrix(matrix: List[List[int]], target: int) -> bool:
    """Matrix where each row is sorted and row[0] > previous row's last. O(log(m*n)).

    PATTERN: treat the 2D matrix as a flattened 1D sorted array.
    """
    if not matrix or not matrix[0]:
        return False
    m, n = len(matrix), len(matrix[0])
    lo, hi = 0, m * n - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        val = matrix[mid // n][mid % n]
        if val == target:
            return True
        if val < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return False


def ship_within_days(weights: List[int], days: int) -> int:
    """Minimum ship capacity to ship packages (in order) within `days`.

    PATTERN: BINARY SEARCH ON THE ANSWER. feasible(cap) is monotonic:
    if cap works, every larger cap works.
    COMPLEXITY: O(n log(sum(weights))).
    TALK TRACK: "Same shape as 'smallest cluster / fewest partitions that meets
    the SLA' - search the smallest value where a monotonic check passes."
    """
    def feasible(cap: int) -> bool:
        needed, load = 1, 0
        for w in weights:
            if load + w > cap:
                needed += 1
                load = 0
            load += w
        return needed <= days

    lo, hi = max(weights), sum(weights)
    while lo < hi:
        mid = (lo + hi) // 2
        if feasible(mid):
            hi = mid
        else:
            lo = mid + 1
    return lo


def find_peak_element(nums: List[int]) -> int:
    """Index of any element strictly greater than its neighbors (nums[-1]=nums[n]=-inf). O(log n)."""
    lo, hi = 0, len(nums) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if nums[mid] < nums[mid + 1]:
            lo = mid + 1
        else:
            hi = mid
    return lo
