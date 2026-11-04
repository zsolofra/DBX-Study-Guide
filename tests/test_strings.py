import pytest


@pytest.mark.parametrize("s,expected", [
    ("A man, a plan, a canal: Panama", True),
    ("race a car", False),
    ("", True),
    (".,", True),
])
def test_is_palindrome(strings, s, expected):
    assert strings.is_palindrome(s) is expected


def test_is_anagram(strings):
    assert strings.is_anagram("anagram", "nagaram")
    assert not strings.is_anagram("rat", "car")
    assert not strings.is_anagram("a", "aa")


@pytest.mark.parametrize("s,expected", [
    ("abcabcbb", 3), ("bbbbb", 1), ("pwwkew", 3), ("", 0), ("abba", 2),
])
def test_longest_unique_substring(strings, s, expected):
    assert strings.length_of_longest_unique_substring(s) == expected


@pytest.mark.parametrize("s,valid", [
    ("babad", {"bab", "aba"}), ("cbbd", {"bb"}), ("a", {"a"}), ("", {""}),
])
def test_longest_palindromic_substring(strings, s, valid):
    assert strings.longest_palindromic_substring(s) in valid


@pytest.mark.parametrize("s,t,expected", [
    ("ADOBECODEBANC", "ABC", "BANC"), ("a", "a", "a"), ("a", "aa", ""),
])
def test_min_window_substring(strings, s, t, expected):
    assert strings.min_window_substring(s, t) == expected


def test_compress(strings):
    assert strings.compress("aaabccccc") == "a3b1c5"
    assert strings.compress("abc") == "abc"
    assert strings.compress("") == ""


def test_reverse_words(strings):
    assert strings.reverse_words("  the sky  is blue ") == "blue is sky the"


@pytest.mark.parametrize("s,expected", [
    ("()[]{}", True), ("(]", False), ("([{}])", True), ("(", False), ("]", False),
])
def test_valid_parentheses(strings, s, expected):
    assert strings.valid_parentheses(s) is expected
