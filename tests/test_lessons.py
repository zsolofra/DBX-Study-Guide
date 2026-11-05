"""Every in-lesson exercise's reference solution must pass its own checker."""
import pytest

from levels.lessons import FUNCTION_EXERCISES, LessonChecker


@pytest.mark.parametrize("ex_id", sorted(FUNCTION_EXERCISES))
def test_reference_passes(ex_id):
    assert LessonChecker().check(ex_id, FUNCTION_EXERCISES[ex_id]["ref"])


def test_stub_reports_todo(capsys):
    def stub(*_):
        raise NotImplementedError

    assert not LessonChecker().check("J-1", stub)
    assert "[TODO]" in capsys.readouterr().out


def test_wrong_answer_and_hint(capsys):
    checker = LessonChecker()
    assert not checker.check("J-3", lambda nums: 0)
    checker.hint("J-3")
    checker.solution("J-3")
    out = capsys.readouterr().out
    assert "TRY AGAIN" in out and "HINT:" in out and "def max_consecutive_ones" in out
