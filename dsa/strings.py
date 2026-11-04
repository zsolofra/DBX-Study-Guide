"""Strings: two pointers, sliding window with counts, expand-around-center, stacks."""
from collections import Counter
from typing import List


def is_palindrome(s: str) -> bool:
    """True if s is a palindrome considering only alphanumerics, case-insensitive.

    PATTERN: two pointers skipping non-alnum.
    COMPLEXITY: O(n) time, O(1) space.
    """
    i, j = 0, len(s) - 1
    while i < j:
        if not s[i].isalnum():
            i += 1
        elif not s[j].isalnum():
            j -= 1
        else:
            if s[i].lower() != s[j].lower():
                return False
            i, j = i + 1, j - 1
    return True


def is_anagram(a: str, b: str) -> bool:
    """True if b is a rearrangement of a.

    PATTERN: frequency count (hash map).
    COMPLEXITY: O(n) time, O(k) space for k distinct chars.
    TALK TRACK: "Sorting is O(n log n); counting is O(n)."
    """
    return len(a) == len(b) and Counter(a) == Counter(b)


def length_of_longest_unique_substring(s: str) -> int:
    """Length of the longest substring without repeating characters.

    PATTERN: sliding window + last-seen index map.
    COMPLEXITY: O(n) time, O(min(n, alphabet)) space.
    """
    last_seen = {}
    left = best = 0
    for right, ch in enumerate(s):
        if ch in last_seen and last_seen[ch] >= left:
            left = last_seen[ch] + 1
        last_seen[ch] = right
        best = max(best, right - left + 1)
    return best


def longest_palindromic_substring(s: str) -> str:
    """Longest palindromic substring (first one found on ties).

    PATTERN: expand around each of the 2n-1 centers.
    COMPLEXITY: O(n^2) time, O(1) space. (Manacher's is O(n) - mention it, rarely code it.)
    """
    if not s:
        return ""
    start, end = 0, 0

    def expand(l: int, r: int) -> tuple:
        while l >= 0 and r < len(s) and s[l] == s[r]:
            l, r = l - 1, r + 1
        return l + 1, r - 1

    for c in range(len(s)):
        for l, r in (expand(c, c), expand(c, c + 1)):
            if r - l > end - start:
                start, end = l, r
    return s[start:end + 1]


def min_window_substring(s: str, t: str) -> str:
    """Smallest substring of s containing every char of t (with multiplicity); "" if none.

    PATTERN: sliding window with a 'missing' counter.
    COMPLEXITY: O(|s| + |t|) time.
    """
    if not t or not s:
        return ""
    need = Counter(t)
    missing = len(t)
    left = 0
    best = (0, float("inf"))
    for right, ch in enumerate(s):
        if need[ch] > 0:
            missing -= 1
        need[ch] -= 1
        if missing == 0:
            while need[s[left]] < 0:
                need[s[left]] += 1
                left += 1
            if right - left < best[1] - best[0]:
                best = (left, right)
            need[s[left]] += 1
            missing += 1
            left += 1
    return "" if best[1] == float("inf") else s[best[0]:best[1] + 1]


def compress(s: str) -> str:
    """Run-length encode: 'aaabcc' -> 'a3b1c2'. Return s if encoding is not shorter.

    PATTERN: single pass, build list then ''.join (never += in a loop).
    COMPLEXITY: O(n) time.
    TALK TRACK: "This is literally what Parquet RLE/dictionary encoding does -
    and why sorting/clustering data improves compression."
    """
    if not s:
        return s
    parts: List[str] = []
    run_char, run_len = s[0], 1
    for ch in s[1:]:
        if ch == run_char:
            run_len += 1
        else:
            parts.append(f"{run_char}{run_len}")
            run_char, run_len = ch, 1
    parts.append(f"{run_char}{run_len}")
    out = "".join(parts)
    return out if len(out) < len(s) else s


def reverse_words(s: str) -> str:
    """'  the sky  is blue ' -> 'blue is sky the'."""
    return " ".join(reversed(s.split()))


def valid_parentheses(s: str) -> bool:
    """True if every bracket ()[]{} is closed in correct order.

    PATTERN: stack.
    COMPLEXITY: O(n) time, O(n) space.
    """
    pairs = {")": "(", "]": "[", "}": "{"}
    stack: List[str] = []
    for ch in s:
        if ch in "([{":
            stack.append(ch)
        elif ch in pairs:
            if not stack or stack.pop() != pairs[ch]:
                return False
    return not stack
