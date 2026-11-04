"""Test cases, validators and multiple-choice keys for the level tests. SPOILERS."""


def _eq(args, got, expected):
    return got == expected


def _lists(args, got, expected):
    return [list(x) for x in (got or [])] == [list(x) for x in expected]


def _topo(args, got, expected):
    n, prereqs = args
    if expected == "cycle":
        return got == []
    if sorted(got) != list(range(n)):
        return False
    pos = {c: i for i, c in enumerate(got)}
    return all(pos[pre] < pos[course] for course, pre in prereqs)


def _run_lru(cls, args):
    capacity, ops = args
    cache, out = cls(capacity), []
    for op in ops:
        if op[0] == "put":
            cache.put(op[1], op[2])
        else:
            out.append(cache.get(op[1]))
    return out


# qid -> {"cases": [(args_tuple, expected), ...], "validator": fn, "runner": fn}
FUNCTION_CASES = {
    # ---------------- junior ----------------
    "J1": {"cases": [(([1, 2, 3, 1],), True), (([1, 2, 3, 4],), False), (([],), False), (([7],), False),
                     (([1, 1, 1, 3, 3, 4, 3, 2, 4, 2],), True)]},
    "J2": {"cases": [(("anagram", "nagaram"), True), (("rat", "car"), False), (("a", "ab"), False),
                     (("", ""), True), (("aacc", "ccac"), False)]},
    "J3": {"cases": [(([-1, 0, 3, 5, 9, 12], 9), 4), (([-1, 0, 3, 5, 9, 12], 2), -1), (([], 5), -1),
                     (([5], 5), 0), (([1, 3], 3), 1), (([1, 3], 0), -1)]},
    "J4": {"cases": [(([7, 1, 5, 3, 6, 4],), 5), (([7, 6, 4, 3, 1],), 0), (([2],), 0), (([],), 0),
                     (([3, 2, 6, 5, 0, 3],), 4)]},
    "J5": {"cases": [
        (({"A": ["B", "C"], "B": ["D"], "C": ["D", "E"], "D": ["F"], "E": [], "F": []}, "A"),
         {"A": 0, "B": 1, "C": 1, "D": 2, "E": 2, "F": 3}),
        (({"A": ["B"], "B": [], "E": []}, "E"), {"E": 0}),
        (({1: [2], 2: [3], 3: [1]}, 1), {1: 0, 2: 1, 3: 2}),
    ]},
    "J6": {"cases": [(("Bob hit a ball, the hit BALL flew far after it was hit.",), "hit"),
                     (("a b b a c",), "a"), (("",), ""), (("Data, data! DATA?",), "data")]},
    # ---------------- mid ----------------
    "M1": {"cases": [(("abcabcbb",), 3), (("bbbbb",), 1), (("pwwkew",), 3), (("",), 0), (("abba",), 2),
                     (("dvdf",), 3)]},
    "M2": {"cases": [(([1, 1, 1], 2), 2), (([1, 2, 3], 3), 2), (([1, -1, 0], 0), 3),
                     (([3, 4, 7, 2, -3, 1, 4, 2], 7), 4), (([], 0), 0)]},
    "M3": {"cases": [(([3, 6, 7, 11], 8), 4), (([30, 11, 23, 4, 20], 5), 30), (([30, 11, 23, 4, 20], 6), 23),
                     (([1], 1), 1), (([1_000_000_000], 2), 500_000_000)]},
    "M4": {"validator": _topo, "cases": [
        ((2, [[1, 0]]), "valid"), ((2, [[1, 0], [0, 1]]), "cycle"),
        ((4, [[1, 0], [2, 0], [3, 1], [3, 2]]), "valid"), ((1, []), "valid"),
        ((3, [[0, 1], [1, 2], [2, 0]]), "cycle"),
    ]},
    "M5": {"cases": [((["i", "love", "leetcode", "i", "love", "coding"], 2), ["i", "love"]),
                     ((["the", "day", "is", "sunny", "the", "the", "the", "sunny", "is", "is"], 4),
                      ["the", "is", "sunny", "day"]),
                     ((["b", "a", "c"], 2), ["a", "b"])]},
    "M6": {"validator": _lists, "cases": [
        (([[1, 3], [2, 6], [8, 10], [15, 18]],), [[1, 6], [8, 10], [15, 18]]),
        (([[1, 4], [4, 5]],), [[1, 5]]), (([],), []), (([[5, 7], [1, 2]],), [[1, 2], [5, 7]]),
        (([[1, 10], [2, 3], [4, 5]],), [[1, 10]]),
    ]},
    # ---------------- senior ----------------
    "S1": {"cases": [(([[2, 1, 1], [2, 3, 1], [3, 4, 1]], 4, 2), 2), (([[1, 2, 1]], 2, 1), 1),
                     (([[1, 2, 1]], 2, 2), -1), (([[1, 2, 4], [1, 3, 1], [3, 2, 1]], 3, 1), 2)]},
    "S2": {"runner": _run_lru, "cases": [
        ((2, [("put", 1, 1), ("put", 2, 2), ("get", 1), ("put", 3, 3), ("get", 2), ("put", 4, 4),
              ("get", 1), ("get", 3), ("get", 4)]), [1, -1, -1, 3, 4]),
        ((1, [("put", 1, 1), ("put", 1, 10), ("get", 1), ("put", 2, 2), ("get", 1), ("get", 2)]), [10, -1, 2]),
        ((2, [("put", 1, 1), ("put", 2, 2), ("put", 1, 100), ("put", 3, 3), ("get", 2), ("get", 1)]), [-1, 100]),
    ]},
    "S3": {"cases": [
        (([("a", 1, 5), ("a", 5, 9), ("b", 1, 5), ("b", 4, 9), ("c", 1, None), ("c", 3, 4), ("d", 10, None)],),
         ["b", "c"]),
        (([("x", 5, None), ("x", 1, 5)],), []), (([("y", 1, None), ("y", 2, None)],), ["y"]), (([],), []),
    ]},
    "S4": {"cases": [(([(0, 30), (5, 10), (15, 20)],), 2), (([(7, 10), (2, 4)],), 1), (([(1, 5), (5, 10)],), 1),
                     (([],), 0), (([(1, 10), (2, 9), (3, 8)],), 3)]},
    "S5": {"cases": [
        (({"r1": ["a@x", "555"], "r2": ["b@x"], "r3": ["555", "c@x"], "r4": ["c@x"], "r5": []},),
         [["r1", "r3", "r4"], ["r2"], ["r5"]]),
        (({"a": ["1"], "b": ["2"], "c": ["1", "2"]},), [["a", "b", "c"]]),
    ]},
}

MCQ = {
    "junior": {"Q1": "c", "Q2": "d", "Q3": "a", "Q4": "c", "Q5": "b", "Q6": "d", "Q7": "a", "Q8": "c",
               "Q9": "b", "Q10": "c"},
    "mid": {"Q1": "b", "Q2": "c", "Q3": "a", "Q4": "c", "Q5": "b", "Q6": "a", "Q7": "b", "Q8": "c"},
    "senior": {"Q1": "b", "Q2": "c", "Q3": "d", "Q4": "a", "Q5": "c", "Q6": "a"},
}
