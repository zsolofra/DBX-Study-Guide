# Databricks Delivery Solutions Architect - Interview Prep Lab

A hands-on test bed for the technical interview. You write code, test yourself, and practice
explaining optimizations. It runs locally (pure Python) and in a **Databricks Free Edition**
workspace (serverless).

| Area | What's here |
|---|---|
| **DS&A** (Arrays, Strings, Binary Search, Graphs, Hash Maps) | `dsa/` reference solutions with pattern, complexity and talk-track docstrings. `exercises/` blank stubs to implement. `tests/` has 80 tests. |
| **Python complexity & optimization** | `dsa/python_optimizations.py` (7 slow/fast pairs), `dsa/benchmark.py` (empirical Big-O fitting) |
| **PySpark optimization** | `spark_lab/` helpers + `notebooks/03_pyspark_optimization_lab.ipynb`: plans, joins, skew/salting, shuffle, UDFs, dedupe, small files, liquid clustering |
| **Data Warehousing (spike)** | `warehouse/` SCD1/SCD2 logic + Delta MERGE generator. `notebooks/04_data_warehousing_lab.ipynb` builds a medallion star schema. `docs/data_warehousing_spike.md` is the deep-dive guide. |
| **Reference** | `docs/cheatsheet.md` |

```
dsa/            reference solutions (don't peek until you've tried)
exercises/      YOUR workspace - generated stubs
tests/          pytest suite; runs against dsa/ or exercises/
spark_lab/      datagen, timing/plan helpers, optimization techniques
warehouse/      SCD logic (pure Python) + Delta MERGE SQL builder
levels/         grader, test cases, answer key and reference solutions for the leveled tests
notebooks/      Jupyter notebooks (.ipynb): 00-05 topic labs, 10-31 leveled track, Includes/ setup
docs/           cheat sheet + data warehousing spike guide
tools/          make_exercises.py (reset stubs), py_to_ipynb.py (convert Databricks .py notebooks), notebook_audit.py (check markdown and code strictly alternate), build_dsa_walkthrough.py (rebuild notebook 06 after editing `dsa/`)
```

---

## 1. Get it into Databricks Free Edition

Sign up at https://www.databricks.com/learn/free-edition (serverless only, which is all this needs).

**Option A - Git folder (recommended: edits sync, easy reset)**
1. Push this folder to a GitHub repo (private is fine).
2. In Databricks: **Workspace -> your user folder -> Create -> Git folder**, paste the repo URL.
3. Open `notebooks/00_setup` and **Run all**.

**Option B - Databricks CLI (no GitHub)**
```bash
databricks workspace import-dir . /Workspace/Users/<you>/databricks-interview-prep
```

**Option C - ZIP upload**: zip the folder, then **Workspace -> Import -> File** and pick the .zip.

> Notebooks find the libraries by adding the repo root (`..` from `notebooks/`) to `sys.path`.
> Keep the folder structure intact.

**Notebook format:** every notebook is a Jupyter `.ipynb` that strictly alternates *markdown, code, markdown, code*, so each code cell sits between two explanations. Written answers go in code cells as text. Databricks opens
`.ipynb` files as notebooks. The `%sql`, `%pip` and `%run` cells are Databricks magics, so run the notebooks in Databricks rather than
plain Jupyter. Run `python tools/notebook_audit.py` after editing to confirm the alternation still holds (`tests/test_notebooks.py` enforces it too).

## 2. Run order

| Notebook | Time | Purpose |
|---|---|---|
| `00_setup` | 2 min | install pytest, smoke-test reference solutions, create schema `workspace.interview_prep` |
| `01_dsa_practice` | ongoing | run tests against **your** code, draw a random mock-interview problem |
| `02_python_complexity_lab` | 15 min | prove Big-O empirically, see slow vs fast Python |
| `03_pyspark_optimization_lab` | 30-45 min | the 5 S's: Spill, Skew, Shuffle, Storage, Serialization |
| `04_data_warehousing_lab` | 45-60 min | bronze -> silver -> star schema, SCD2 MERGE, point-in-time fact load, constraints, BI SQL |
| `05_spot_the_antipattern` | 45 min | 20 "why is this code not optimal?" brain teasers (unnecessary Python <-> PySpark mixing, 2 traps). Diagnose them before opening `05_spot_the_antipattern_ANSWERS`, which has fixes and bad-vs-good timings |
| `06_dsa_code_walkthrough` | ~3 h, or one part at a time | every function in `dsa/`: the problem, the idea, the complexity, the real source, a runnable example, and what to notice. Contains the `exercises/` solutions, so try those first if you want to test yourself |

## 3. Leveled track: walkthrough, then test (Junior -> Mid -> Senior)

Each level is a **class notebook** followed by a **self-grading test**. The classes are built like a Databricks Academy course:
a `%run ./Includes/Classroom-Setup` cell, learning objectives, and numbered lessons (use the notebook's **Table of contents** to jump between them).
Each lesson runs **CONCEPT, then DEMO, then Breakdown, then KNOWLEDGE CHECK**, one idea per cell, and ends with a **YOUR TURN** exercise that checks itself
(`lesson.hint("J-1")` if you're stuck, `lesson.solution("J-1")` to see the answer).
Tests score coding problems against test cases, compare Spark answers to the expected result, score multiple choice, and
(at Senior) take your rubric-based scores for written scenarios. They end with a scorecard and a verdict. **Pass = 85%**, then move up a level.

| Level | Walkthrough | Test | Covers |
|---|---|---|---|
| Junior | `10_junior_walkthrough` | `11_junior_test` (~60 min) | hash maps/sets, two pointers, binary search, BFS; transformations vs actions, join types, built-ins vs UDFs; facts/dims, grain, SCD1 vs SCD2, medallion |
| Mid | `20_mid_walkthrough` | `21_mid_test` (~75 min) | sliding window, prefix sums, binary search on the answer, topological sort, heaps, intervals; window functions, reading plans, skew detection, AQE; SCD2 MERGE, point-in-time joins, Change Data Feed |
| Senior | `30_senior_walkthrough` | `31_senior_test` (~2 h) | Dijkstra, LRU design, sweep line, union-find, overlap checks; salting vs AQE, gaps & islands, sessionization, idempotent writes; SCD2 rebuild with late data; 5 written design/diagnosis scenarios |

Spoilers: solutions are in `levels/reference.py`, and explanations and rubrics in `levels/ANSWER_KEY.md`. The walkthroughs use different data from the tests.

## 4. Self-test workflow (topic drills)

1. Open `exercises/<topic>.py` and implement a function (the docstring is the problem statement).
2. In `01_dsa_practice`, set widgets `target=exercises`, `topic=<topic>`, and run the test cell.
3. Green? Compare with `dsa/<topic>.py` and read the TALK TRACK line out loud.
4. Reset a topic to blank: `python tools/make_exercises.py --force` (resets ALL stubs; add `--hints` to keep pattern hints).

Locally (Windows PowerShell):
```bash
python -m pip install pytest
```
```bash
python -m pytest tests -q
```
```powershell
$env:DSA_TARGET="exercises"; python -m pytest tests/test_arrays.py -q
```

## 5. Free Edition / serverless gotchas (good interview material too)
- No `sparkContext` / RDD API, so use DataFrame APIs (`spark_partition_id()` instead of `getNumPartitions()`).
- `df.cache()` isn't supported. Materialize to a Delta table instead.
- Most `spark.conf` keys are locked (AQE, Photon and autoscaling are managed). The labs use `try_set_conf` and report what they can't set.
- Compute quotas are small. Start with the default `n_orders` (3M) and scale up once it runs.
- Serverless timings are noisy. Compare relative differences, and use the 2nd+ run.

## 6. Suggested 7-day plan
(Alternative: do the leveled track, one level every two days, retaking missed questions the next morning.)
| Day | Focus |
|---|---|
| 1 | Arrays + Hash Maps exercises; `02_python_complexity_lab` |
| 2 | Strings + Binary Search exercises; cheat sheet patterns |
| 3 | Graphs exercises; mock problem x2 (timed, out loud) |
| 4 | `03_pyspark_optimization_lab`: practice every talk track until you can say it without notes; then `05_spot_the_antipattern` |
| 5 | `04_data_warehousing_lab` end to end; implement `exercises/scd.py` from scratch |
| 6 | DW spike guide sections 6-7 (migration playbook + practice prompts); do stretch exercises 1-4 |
| 7 | Full mock: 1 coding problem + 1 optimization diagnosis + 1 DW design, 60 min total |
