"""Notebook layout rules: markdown and code strictly alternate, and answers stay hidden behind code."""
import json
import re
from pathlib import Path

import pytest

NOTEBOOKS = sorted((Path(__file__).resolve().parents[1] / "notebooks").rglob("*.ipynb"))


def cells_of(path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    return [(c["cell_type"], "".join(c["source"])) for c in nb["cells"]]


def test_notebooks_exist():
    assert len(NOTEBOOKS) == 14


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.stem)
def test_strict_alternation(path):
    kinds = [k for k, _ in cells_of(path)]
    assert kinds[0] == "markdown" and kinds[-1] == "markdown"
    assert all(a != b for a, b in zip(kinds, kinds[1:]))


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.stem)
def test_python_cells_compile(path):
    for kind, text in cells_of(path):
        if kind == "code" and not text.lstrip().startswith("%"):
            compile(text, path.name, "exec")


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.stem)
def test_knowledge_check_answers_are_not_in_the_question_cell(path):
    for kind, text in cells_of(path):
        if kind == "markdown" and "KNOWLEDGE CHECK" in text:
            after_question = text.split("KNOWLEDGE CHECK", 1)[1]
            assert not re.search(r"\*\*Answer", after_question), text[:80]
