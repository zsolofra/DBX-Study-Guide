"""Check that every notebook strictly alternates: markdown, code, markdown, code, ..., markdown.

    python tools/notebook_audit.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_cells(path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    return [(c["cell_type"], "".join(c["source"])) for c in nb["cells"]]


def problems(cells):
    found = []
    if cells[0][0] != "markdown":
        found.append("does not start with markdown")
    if cells[-1][0] != "markdown":
        found.append("does not end with markdown")
    for i in range(1, len(cells)):
        if cells[i][0] == cells[i - 1][0]:
            first = next((l for l in cells[i][1].splitlines() if l.strip()), "")[:70]
            found.append(f"cell {i}: two {cells[i][0]} cells in a row: {first}")
    return found


def main():
    total = 0
    for path in sorted((ROOT / "notebooks").rglob("*.ipynb")):
        cells = load_cells(path)
        found = problems(cells)
        total += len(found)
        print(f"{path.relative_to(ROOT)}: {len(cells)} cells, {len(found)} problems")
        for p in found:
            print(f"    {p}")
    print(f"TOTAL problems: {total}")
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
