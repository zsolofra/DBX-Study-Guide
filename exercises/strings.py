# AUTO-GENERATED EXERCISE STUBS - implement each function, then run the tests.
"""Strings: two pointers, sliding window with counts, expand-around-center, stacks."""
from collections import Counter
from typing import List

def is_palindrome(s: str) -> bool:
    """True if s is a palindrome considering only alphanumerics, case-insensitive.

    COMPLEXITY: O(n) time, O(1) space.
    """
    raise NotImplementedError('TODO: implement me')

def is_anagram(a: str, b: str) -> bool:
    """True if b is a rearrangement of a.

    COMPLEXITY: O(n) time, O(k) space for k distinct chars.
    """
    raise NotImplementedError('TODO: implement me')

def length_of_longest_unique_substring(s: str) -> int:
    """Length of the longest substring without repeating characters.

    COMPLEXITY: O(n) time, O(min(n, alphabet)) space.
    """
    raise NotImplementedError('TODO: implement me')

def longest_palindromic_substring(s: str) -> str:
    """Longest palindromic substring (first one found on ties).

    COMPLEXITY: O(n^2) time, O(1) space. (Manacher's is O(n) - mention it, rarely code it.)
    """
    raise NotImplementedError('TODO: implement me')

def min_window_substring(s: str, t: str) -> str:
    """Smallest substring of s containing every char of t (with multiplicity); "" if none.

    COMPLEXITY: O(|s| + |t|) time.
    """
    raise NotImplementedError('TODO: implement me')

def compress(s: str) -> str:
    """Run-length encode: 'aaabcc' -> 'a3b1c2'. Return s if encoding is not shorter.

    COMPLEXITY: O(n) time.
    """
    raise NotImplementedError('TODO: implement me')

def reverse_words(s: str) -> str:
    """'  the sky  is blue ' -> 'blue is sky the'.
    """
    raise NotImplementedError('TODO: implement me')

def valid_parentheses(s: str) -> bool:
    """True if every bracket ()[]{} is closed in correct order.

    COMPLEXITY: O(n) time, O(n) space.
    """
    raise NotImplementedError('TODO: implement me')
