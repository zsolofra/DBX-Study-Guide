"""Sanity checks for the level tests: every reference solution must score 100%,
and a stub must register as 'not attempted'."""
import pytest

from levels import cases as C
from levels import reference as R
from levels.grader import Scorecard

REFERENCE_FOR = {
    "J1": R.contains_duplicate, "J2": R.is_anagram, "J3": R.binary_search, "J4": R.max_profit,
    "J5": R.bfs_levels, "J6": R.most_common_word,
    "M1": R.length_of_longest_substring, "M2": R.subarray_sum_k, "M3": R.min_eating_speed,
    "M4": R.course_order, "M5": R.top_k_frequent_words, "M6": R.merge_intervals,
    "S1": R.network_delay_time, "S2": R.LRUCache, "S3": R.overlapping_keys, "S4": R.min_concurrent,
    "S5": R.resolve_entities,
}


def test_every_case_has_a_reference():
    assert set(REFERENCE_FOR) == set(C.FUNCTION_CASES)


@pytest.mark.parametrize("qid", sorted(REFERENCE_FOR))
def test_reference_scores_full_marks(qid, capsys):
    card = Scorecard("junior")
    card.grade(qid, REFERENCE_FOR[qid])
    earned, possible, note = card.results[qid]
    assert earned == possible, note


def test_stub_is_not_attempted():
    def stub(*_):
        raise NotImplementedError

    card = Scorecard("junior")
    card.grade("J1", stub)
    assert card.results["J1"][2] == "not attempted"


def test_wrong_answer_partial_credit():
    card = Scorecard("junior")
    card.grade("J4", lambda prices: 0)          # right only when there's no profit
    earned, _, _ = card.results["J4"]
    assert 0 < earned < 1


def test_mcq_and_summary():
    card = Scorecard("mid")
    card.mcq("Q1", C.MCQ["mid"]["Q1"].upper())
    card.mcq("Q2", "z")
    card.self_score("W1", 4)
    assert card.summary() == pytest.approx(100 * 1.8 / 3)
