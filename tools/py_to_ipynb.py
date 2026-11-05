"""Convert Databricks source-format notebooks (.py) to Jupyter notebooks (.ipynb).

    python tools/py_to_ipynb.py notebooks/03_pyspark_optimization_lab.py [...]

- `# MAGIC %md` cells become markdown cells
- `# MAGIC %sql` / `%pip` / `%run` cells become code cells that keep the magic (Databricks runs them as-is)
- everything else becomes a Python code cell
The .ipynb is written next to the .py; the .py is left in place.
"""
import hashlib
import json
import sys
from pathlib import Path

HEADER = "# Databricks notebook source"
SEP = "# COMMAND ----------"


def _strip_magic(line):
    if line.startswith("# MAGIC "):
        return line[len("# MAGIC "):]
    if line.startswith("# MAGIC"):
        return line[len("# MAGIC"):]
    return line


def parse_cells(text):
    text = text.lstrip("﻿")
    if text.startswith(HEADER):
        text = text[len(HEADER):]
    cells = []
    for raw in text.split(SEP):
        lines = raw.strip("\n").splitlines()
        if not any(l.strip() for l in lines):
            continue
        if lines[0].startswith("# MAGIC %md"):
            body = [_strip_magic(l) for l in lines]
            first = body[0][len("%md"):].strip()
            body = ([first] if first else []) + body[1:]
            cells.append(("markdown", body))
        elif lines[0].startswith("# MAGIC %"):
            cells.append(("code", [_strip_magic(l) for l in lines]))
        else:
            cells.append(("code", lines))
    return cells


def to_source(lines):
    while lines and not lines[-1].strip():
        lines = lines[:-1]
    return [l + "\n" for l in lines[:-1]] + ([lines[-1]] if lines else [])


def build_notebook(cells, name):
    nb_cells = []
    for i, (kind, lines) in enumerate(cells):
        cell_id = hashlib.sha1(f"{name}:{i}".encode()).hexdigest()[:12]   # stable ids keep git diffs small
        cell = {"cell_type": kind, "id": cell_id, "metadata": {}, "source": to_source(lines)}
        if kind == "code":
            cell.update({"execution_count": None, "outputs": []})
        nb_cells.append(cell)
    return {
        "cells": nb_cells,
        "metadata": {
            "application/vnd.databricks.v1+notebook": {"notebookName": name, "language": "python"},
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def write_notebook(nb, path):
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def convert(py_path):
    py_path = Path(py_path)
    nb = build_notebook(parse_cells(py_path.read_text(encoding="utf-8")), py_path.stem)
    out = py_path.with_suffix(".ipynb")
    write_notebook(nb, out)
    return out


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print("wrote", convert(p))
