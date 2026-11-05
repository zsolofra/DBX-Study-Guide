"""List code cells that are not directly preceded by a markdown explanation.

    python tools/notebook_audit.py            # audits notebooks/**/*.py (Databricks source) or *.ipynb
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEP = "# COMMAND ----------"


def cells_from_py(path):
    text = path.read_text(encoding="utf-8").replace("# Databricks notebook source\n", "", 1)
    out = []
    for raw in text.split(SEP):
        body = raw.strip("\n")
        if not body.strip():
            continue
        kind = "markdown" if body.lstrip().startswith("# MAGIC %md") else "code"
        out.append((kind, body))
    return out


def cells_from_ipynb(path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    return [(c["cell_type"], "".join(c["source"])) for c in nb["cells"]]


def audit(path):
    cells = cells_from_py(path) if path.suffix == ".py" else cells_from_ipynb(path)
    gaps = []
    for i, (kind, body) in enumerate(cells):
        if kind == "code" and (i == 0 or cells[i - 1][0] != "markdown"):
            first = next((l for l in body.splitlines() if l.strip()), "")[:70]
            gaps.append((i, first))
    return len(cells), gaps


def main():
    files = sorted((ROOT / "notebooks").rglob("*.ipynb")) or sorted((ROOT / "notebooks").rglob("*.py"))
    total = 0
    for f in files:
        n, gaps = audit(f)
        total += len(gaps)
        print(f"{f.relative_to(ROOT)}: {n} cells, {len(gaps)} code cells without an explanation above")
        for i, first in gaps:
            print(f"    cell {i}: {first}")
    print(f"TOTAL gaps: {total}")
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
