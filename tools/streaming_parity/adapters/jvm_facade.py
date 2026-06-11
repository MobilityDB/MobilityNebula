#!/usr/bin/env python3
"""JVM-facade adapter (Flink / Kafka) for the streaming-parity harness.

The Flink/Kafka codegen emits a tier-aware MEOS facade as
`MeosOps<TypeGroup>.java` classes whose `public static <name>(…)` methods are
named after the MEOS function. So:

  L2 WIRED  — every `public static` facade method name.
  L3 PROVEN — a wired method exercised by a JUnit test that PASSED. Pass the
              passing methods (one MEOS-fn name per line) via --passing, derived
              from the Surefire/JUnit report of an ACCUMULATED-PR Maven build.
              With no per-function tests, L3 is empty — the honest result.

Usage:
  jvm_facade.py --facade-dir <…/meos> [--passing passing.txt] > feed.tsv
"""
from __future__ import annotations
import argparse
import os
import re
import sys

METHOD = re.compile(r"public static [A-Za-z0-9_<>\[\].,? ]+?\b([a-z][a-z0-9_]+)\s*\(")


def facade_methods(d):
    names = set()
    for root, _, files in os.walk(d):
        for f in files:
            if f.startswith("MeosOps") and f.endswith(".java"):
                for m in METHOD.finditer(open(os.path.join(root, f)).read()):
                    names.add(m.group(1))
    return names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--facade-dir", required=True)
    ap.add_argument("--passing", help="file: one passing-tested MEOS fn name per line")
    a = ap.parse_args()
    wired = facade_methods(a.facade_dir)
    proven = set()
    if a.passing and os.path.exists(a.passing):
        proven = {ln.strip() for ln in open(a.passing) if ln.strip()} & wired
    for fn in sorted(wired):
        print(f"{fn}\t{'proven' if fn in proven else 'wired'}")
    sys.stderr.write(f"[jvm adapter] wired={len(wired)} proven={len(proven)}\n")


if __name__ == "__main__":
    main()
