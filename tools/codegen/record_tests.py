#!/usr/bin/env python3
"""Auto-record expected results for generated MEOS systests.

The codegen waves emit one .test per operator with a *placeholder* expected
block. Recording the real expected output by hand (run, read the diff, paste the
actual) does not scale to hundreds of operators, so this does it mechanically:

  for each .test:
    1. blank the expected block (everything after the final `----`);
    2. run the test in the container — with no expected rows every actual row is
       reported by the systest as an excess line `<exp> | <actual>` (fields
       space-separated, expected side shown as `_`);
    3. parse the ACTUAL side of those lines, comma-join the fields into rows;
    4. write the rows back as the expected block;
    5. re-run and confirm `All queries passed`.

A test that already passes is left untouched. A test that fails for a reason
other than a recordable mismatch (compile/parse/schema error, exception 9999) is
reported and skipped, never silently "recorded".

Usage:
  python3 tools/codegen/record_tests.py --tests nes-systests/function/meos/foo.test [more.test ...]
  python3 tools/codegen/record_tests.py --tests-dir nes-systests/function/meos --only foo,bar
"""
from __future__ import annotations
import argparse
import os
import re
import subprocess
import sys

IMAGE_DEFAULT = "localhost/nes-development:mobilitynebula-v17"
BUILD_DIR_DEFAULT = "build-w15"
PASS_MARK = "All queries passed"
SEP = "----"
# A result-diff line: "<expected fields> | <actual fields>". The actual side is
# what we record. Schema/explanatory notes contain '(' — skip them.
DIFF_RE = re.compile(r"^(?P<exp>.*?) \| (?P<act>.+)$")


def run_test(repo, image, build_dir, testfile, timeout=600):
    cmd = ["docker", "run", "--rm", "-v", f"{repo}:/workspace", "-w", "/workspace",
           image, "bash", "-lc",
           f"./{build_dir}/nes-systests/systest/systest --testLocation {testfile} 2>&1"]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return p.stdout + p.stderr


def split_expected(text):
    """Return (head_including_separator, expected_block_lines). The expected
    block is everything after the LAST line equal to '----'."""
    lines = text.split("\n")
    idx = max((i for i, l in enumerate(lines) if l.strip() == SEP), default=None)
    if idx is None:
        return None, None
    return "\n".join(lines[:idx + 1]), lines[idx + 1:]


def parse_actual_rows(output):
    """Pull the actual-side values out of the systest result diff. Returns a list
    of comma-joined rows in printed order, or None if no diff lines were found."""
    rows = []
    for line in output.split("\n"):
        m = DIFF_RE.match(line.rstrip())
        if not m:
            continue
        act = m.group("act").strip()
        # Skip the result-table header ("Expected Results(Sorted) | Actual Results(Sorted)").
        if act.startswith("Actual Results"):
            continue
        # Skip schema-mismatch / explanatory notes (e.g. "... (expected sink ...)").
        # MEOS value literals also carry parentheses (NPoint(1 0.5), STBOX(...),
        # {NSegment(...)}), so only drop a paren-bearing line when its text reads
        # like an English explanatory note, not a recordable value.
        low = act.lower()
        NOTE_MARKERS = ("expected", "schema", "sink", "unsupported", "mismatch",
                        "error", "cannot", "unknown", "no such", "unable")
        if "(" in act and ")" in act and any(k in low for k in NOTE_MARKERS):
            continue
        fields = act.split()
        if not fields or all(f == "_" for f in fields):
            continue
        # Temporal values print as "<id> <value>@<timestamp>", where the trailing
        # timestamp carries internal spaces (e.g. "2021-01-01 00:00:00+00") that
        # the systest comparison preserves. Commas join the top-level fields and
        # any value sub-parts (NPoint(1 0.5) -> NPoint(1,0.5)) BEFORE the '@', but
        # the timestamp keeps its spaces. Rows with no '@' are plain comma-joins.
        at = act.find("@")
        if at != -1:
            rows.append(",".join(act[:at].split()) + "@" + act[at + 1:].strip())
        else:
            rows.append(",".join(fields))
    return rows or None


def record_one(repo, image, build_dir, path):
    with open(path) as f:
        original = f.read()
    # 1. already green?
    out = run_test(repo, image, build_dir, os.path.relpath(path, repo))
    if PASS_MARK in out:
        return "pass", "already passing"
    head, _ = split_expected(original)
    if head is None:
        return "skip", "no '----' separator"
    # 2. blank the expected block, run to surface all actual rows
    blanked = head + "\n"
    with open(path, "w") as f:
        f.write(blanked)
    out = run_test(repo, image, build_dir, os.path.relpath(path, repo))
    rows = parse_actual_rows(out)
    if not rows:
        with open(path, "w") as f:  # restore
            f.write(original)
        reason = "exception 9999" if "9999" in out else (
            "schema mismatch" if "!=" in out else "no recordable actual rows")
        return "skip", reason
    # 3. write recorded rows, 4. confirm
    recorded = head + "\n" + "\n".join(rows) + "\n"
    with open(path, "w") as f:
        f.write(recorded)
    out = run_test(repo, image, build_dir, os.path.relpath(path, repo))
    if PASS_MARK in out:
        return "recorded", f"{len(rows)} row(s)"
    with open(path, "w") as f:  # restore on failure to confirm
        f.write(original)
    return "fail", "recorded rows did not pass on re-run"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.getcwd())
    ap.add_argument("--image", default=IMAGE_DEFAULT)
    ap.add_argument("--build-dir", default=BUILD_DIR_DEFAULT)
    ap.add_argument("--tests", nargs="*", default=[], help="explicit .test paths")
    ap.add_argument("--tests-dir", help="record every .test in this dir")
    ap.add_argument("--only", help="comma-separated basenames to restrict --tests-dir")
    a = ap.parse_args()

    paths = list(a.tests)
    if a.tests_dir:
        only = set(a.only.split(",")) if a.only else None
        for f in sorted(os.listdir(a.tests_dir)):
            if f.endswith(".test") and (only is None or f[:-5] in only):
                paths.append(os.path.join(a.tests_dir, f))
    if not paths:
        ap.error("no tests given (use --tests or --tests-dir)")

    tally = {}
    for p in paths:
        status, detail = record_one(a.repo, a.image, a.build_dir, os.path.abspath(p))
        tally[status] = tally.get(status, 0) + 1
        print(f"  [{status:8s}] {os.path.basename(p):45s} {detail}")
    print("summary: " + ", ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    return 0 if not tally.get("fail") else 1


if __name__ == "__main__":
    sys.exit(main())
