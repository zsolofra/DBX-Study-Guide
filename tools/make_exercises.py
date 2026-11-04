"""Generate blank exercise stubs from the reference solutions in dsa/ and warehouse/.

Every function/method body becomes: docstring (problem statement) + raise NotImplementedError.
By default PATTERN / TALK TRACK / INVARIANT hints are stripped so you have to
come up with the approach yourself; COMPLEXITY stays as your target.

Usage:
    python tools/make_exercises.py            # create missing stub files only
    python tools/make_exercises.py --force    # overwrite (resets your work!)
    python tools/make_exercises.py --hints    # keep the PATTERN hints
"""
import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCES = {
    "dsa": ["arrays", "strings", "binary_search", "graphs", "hash_maps"],
    "warehouse": ["scd"],
}
HIDDEN_TAGS = ("PATTERN:", "TALK TRACK:", "INVARIANT:")
ANY_TAG = HIDDEN_TAGS + ("COMPLEXITY:", "NOTE:")


def strip_hints(doc: str) -> str:
    out, skipping = [], False
    for line in doc.splitlines():
        s = line.strip()
        if s.startswith(HIDDEN_TAGS):
            skipping = True
            continue
        if skipping and (not s or s.startswith(ANY_TAG)):
            skipping = False
        if not skipping:
            out.append(line)
    return "\n".join(out).rstrip() + "\n    "


class Stubber(ast.NodeTransformer):
    def __init__(self, keep_hints: bool) -> None:
        self.keep_hints = keep_hints

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        doc = ast.get_docstring(node, clean=False)
        body = []
        if doc:
            if not self.keep_hints:
                doc = strip_hints(doc)
            body.append(ast.Expr(ast.Constant(doc)))
        body.append(ast.parse("raise NotImplementedError('TODO: implement me')").body[0])
        node.body = body
        return node


def stub_source(src: str, keep_hints: bool) -> str:
    tree = Stubber(keep_hints).visit(ast.parse(src))
    ast.fix_missing_locations(tree)
    header = "# AUTO-GENERATED EXERCISE STUBS - implement each function, then run the tests.\n"
    return header + ast.unparse(tree) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="overwrite existing stubs")
    ap.add_argument("--hints", action="store_true", help="keep PATTERN/TALK TRACK hints")
    args = ap.parse_args()

    out_dir = ROOT / "exercises"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "__init__.py").write_text('"""Your workspace: implement these, then run tests with DSA_TARGET=exercises."""\n')
    for pkg, modules in SOURCES.items():
        for mod in modules:
            dest = out_dir / f"{mod}.py"
            if dest.exists() and not args.force:
                print(f"skip   {dest.relative_to(ROOT)} (exists; use --force to reset)")
                continue
            src = (ROOT / pkg / f"{mod}.py").read_text()
            dest.write_text(stub_source(src, args.hints))
            print(f"wrote  {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
