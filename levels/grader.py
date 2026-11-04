"""Self-grading scorecard used by the level test notebooks.

    from levels.grader import Scorecard
    card = Scorecard("junior", spark)      # spark optional for pure-Python grading
    card.grade("J1", contains_duplicate)   # coding problem
    card.grade_df("JS1", my_df)            # Spark problem, compared to the reference result
    card.mcq("Q1", "c")                    # multiple choice
    card.self_score("W1", 4, out_of=5)     # written answer, scored with the rubric in ANSWER_KEY.md
    card.summary()
"""
import copy
from decimal import Decimal

from levels import cases as C
from levels import reference as R

SECTIONS = {"J": "coding", "M": "coding", "S": "coding", "Q": "multiple choice", "W": "written"}


def _section(qid):
    if qid[1:2] == "S" and qid[0] in "JMS":
        return "spark"
    return SECTIONS.get(qid[0], "other")


def _norm(v):
    if isinstance(v, (float, Decimal)):
        return round(float(v), 2)
    return v


class Scorecard:
    def __init__(self, level, spark=None):
        self.level = level
        self.spark = spark
        self.results = {}
        self._data = None

    @property
    def data(self):
        """The level's input DataFrames (dict). Built once."""
        if self._data is None:
            from levels.data import DATASETS
            self._data = DATASETS[self.level](self.spark)
        return self._data

    # ------------------------------------------------------------------ record
    def _record(self, qid, earned, possible, note=""):
        self.results[qid] = (earned, possible, note)
        tag = "PASS" if earned == possible else ("TODO" if note == "not attempted" else "FAIL")
        print(f"[{tag}] {qid}: {earned:g}/{possible:g}  {note}".rstrip())

    # ----------------------------------------------------------- coding tests
    def grade(self, qid, fn):
        spec = C.FUNCTION_CASES[qid]
        validator = spec.get("validator", C._eq)
        runner = spec.get("runner")
        passed, fails = 0, []
        for args, expected in spec["cases"]:
            call_args = copy.deepcopy(args)
            try:
                got = runner(fn, call_args) if runner else fn(*call_args)
            except NotImplementedError:
                return self._record(qid, 0, 1, "not attempted")
            except Exception as e:  # noqa: BLE001
                got = f"raised {type(e).__name__}: {e}"
            if not isinstance(got, str) or not got.startswith("raised"):
                try:
                    ok = validator(args, got, expected)
                except Exception:  # noqa: BLE001
                    ok = False
            else:
                ok = False
            if ok:
                passed += 1
            else:
                fails.append((args, expected, got))
        total = len(spec["cases"])
        note = f"{passed}/{total} cases"
        for args, expected, got in fails[:3]:
            note += f"\n      input={_short(args)}  expected={_short(expected)}  got={_short(got)}"
        self._record(qid, round(passed / total, 2), 1, note)

    # ------------------------------------------------------------ spark tests
    def grade_df(self, qid, df):
        if df is None:
            return self._record(qid, 0, 1, "not attempted")
        ref_fn, inputs, cols = R.SPARK[qid]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return self._record(qid, 0, 1, f"missing columns {missing}; expected exactly {cols}")
        try:
            got = sorted((tuple(_norm(r[c]) for c in cols) for r in df.select(*cols).collect()), key=repr)
        except Exception as e:  # noqa: BLE001
            return self._record(qid, 0, 1, f"query failed: {type(e).__name__}: {str(e)[:200]}")
        expected_df = ref_fn(*[self.data[name] for name in inputs])
        expected = sorted((tuple(_norm(r[c]) for c in cols) for r in expected_df.collect()), key=repr)
        if got == expected:
            return self._record(qid, 1, 1, f"{len(got)} rows match")
        extra = [r for r in got if r not in expected][:3]
        lost = [r for r in expected if r not in got][:3]
        note = f"got {len(got)} rows, expected {len(expected)}"
        if extra:
            note += f"\n      unexpected rows: {extra}"
        if lost:
            note += f"\n      missing rows:    {lost}"
        self._record(qid, 0, 1, note)

    # ---------------------------------------------------------- other scoring
    def mcq(self, qid, answer):
        if not answer:
            return self._record(qid, 0, 1, "not attempted")
        ok = str(answer).strip().lower() == C.MCQ[self.level][qid]
        self._record(qid, 1 if ok else 0, 1, "" if ok else "see ANSWER_KEY.md for the explanation")

    def self_score(self, qid, score, out_of=5):
        if score is None:
            return self._record(qid, 0, 1, "not attempted")
        # written answers are weighted like one coding problem
        self._record(qid, round(min(score, out_of) / out_of, 2), 1, f"self-scored {score}/{out_of} with the rubric")

    # ---------------------------------------------------------------- summary
    def summary(self):
        by_section = {}
        for qid, (earned, possible, _) in self.results.items():
            e, p = by_section.get(_section(qid), (0, 0))
            by_section[_section(qid)] = (e + earned, p + possible)
        total_e = sum(e for e, _ in by_section.values())
        total_p = sum(p for _, p in by_section.values()) or 1
        pct = 100 * total_e / total_p
        print("=" * 60)
        print(f"{self.level.upper()} SCORECARD")
        for sec, (e, p) in by_section.items():
            print(f"  {sec:<16} {e:5.2f} / {p:g}")
        print(f"  {'TOTAL':<16} {total_e:5.2f} / {total_p:g}   ({pct:.0f}%)")
        if pct >= 85:
            verdict = "Ready - move up a level (or redo it timed, out loud)."
        elif pct >= 70:
            verdict = "Close - re-read the walkthrough sections behind the misses, then retake them."
        else:
            verdict = "Redo the walkthrough first, then retake the test tomorrow."
        print(f"  VERDICT: {verdict}")
        todo = [q for q, (e, p, n) in self.results.items() if e < p]
        if todo:
            print(f"  Review: {', '.join(todo)}")
        return pct


def _short(x, n=90):
    s = repr(x)
    return s if len(s) <= n else s[: n - 3] + "..."
