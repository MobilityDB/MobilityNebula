#!/usr/bin/env python3
"""Streaming-parity CI gate (Path-to-100 step 6).

A fast, self-contained guard that operationalizes the measured-not-guessed
discipline. It does NOT re-run the multi-minute callability harness; it checks
the committed feed against two invariants so a regression or an over-claim
fails CI:

  1. NO L3 REGRESSION — confirmed-callable (streamable ∩ proven) must not drop
     below the recorded floor.
  2. NO OVER-CLAIM — the assessment doc may not state "100%" while the gap list
     is non-empty (the exact failure mode this whole methodology was built to
     prevent: "2,097 wired = 100%" measured facade size, not callability).
  3. DOC CONSISTENCY — the L3 count the doc states must match the feed.

Inputs are committed alongside it (feeds/streamable.txt + feeds/*.feed.tsv), so
the gate runs in CI with only Python — no JMEOS jar, no libmeos, no toolchain.

Usage:
  ci_gate.py [--feed feeds/flink-kafka.feed.tsv] [--streamable feeds/streamable.txt]
             [--doc doc/methodology/streaming_parity_assessment.md] [--floor 1794]
Exit non-zero on any violation.
"""
from __future__ import annotations
import argparse
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def load_streamable(path):
    return {ln.strip() for ln in open(path) if ln.strip()}


def load_feed(path):
    proven, wired = set(), set()
    for ln in open(path):
        p = ln.rstrip("\n").split("\t")
        if len(p) >= 2 and p[0]:
            (proven if p[1].strip().lower() == "proven" else wired).add(p[0])
    return proven, wired


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", default=os.path.join(HERE, "feeds/flink-kafka.feed.tsv"))
    ap.add_argument("--streamable", default=os.path.join(HERE, "feeds/streamable.txt"))
    ap.add_argument("--doc", default=os.path.join(HERE, "..", "..", "doc/methodology/streaming_parity_assessment.md"))
    ap.add_argument("--floor", type=int, default=1938,
                    help="minimum confirmed-callable; CI fails below this")
    a = ap.parse_args()

    streamable = load_streamable(a.streamable)
    proven, wired = load_feed(a.feed)
    n_str = len(streamable)
    callable_n = len(streamable & proven)
    wired_only = len(streamable & wired - proven)
    gap = n_str - callable_n - wired_only
    pct = 100.0 * callable_n / n_str if n_str else 0.0

    print(f"streamable={n_str}  L3 callable={callable_n} ({pct:.1f}%)  "
          f"wired-only={wired_only}  gap={gap}  floor={a.floor}")

    violations = []
    # 1. regression floor
    if callable_n < a.floor:
        violations.append(f"L3 REGRESSION: {callable_n} < floor {a.floor}")

    # 2/3. doc cross-checks (best-effort; only if the doc is present)
    doc = os.path.exists(a.doc) and open(a.doc).read() or ""
    if doc:
        # 2. no-overclaim: a headline / platform / callability line may not state a
        #    percentage above what the feed supports while a gap remains. Scoped to
        #    those lines (incl. the per-platform result rows, which name
        #    Flink/Kafka/Nebula but not the word "callable") so the "Path to 100%"
        #    heading and "'100%' claim" methodology references don't false-positive.
        if (wired_only + gap) > 0:
            for ln in doc.splitlines():
                if not re.search(r'callable|flink|kafka|nebula|\bl3\b', ln, re.I):
                    continue
                for pc in re.findall(r'(\d{1,3}(?:\.\d)?)\s*%', ln):
                    if float(pc) > pct + 0.1:
                        violations.append(
                            f"OVER-CLAIM: a callability line states {pc}% > measured "
                            f"{pct:.1f}% while {wired_only + gap} streamable functions "
                            f"are not yet confirmed callable: '{ln.strip()[:70]}'")
                        break
                else:
                    continue
                break
        # 3. consistency: the doc must state the measured count
        if str(callable_n) not in doc.replace(",", ""):
            violations.append(
                f"DOC DRIFT: feed measures {callable_n} confirmed callable, "
                f"but the assessment doc does not state it")

    if violations:
        print("\nFAIL — streaming-parity gate:")
        for v in violations:
            print(f"  ✗ {v}")
        sys.exit(1)
    print("OK — no regression, no over-claim, doc consistent.")


if __name__ == "__main__":
    main()
