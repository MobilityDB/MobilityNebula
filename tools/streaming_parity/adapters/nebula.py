#!/usr/bin/env python3
"""NebulaStream adapter for the streaming-parity harness.

Introspects an ACCUMULATED-PR Nebula build to emit a coverage feed
(`function<TAB>{proven|wired}`) consumed by streaming_parity.py.

  L2 WIRED  — every distinct MEOS function called by a generated/hand-written
              operator (the `meos_call` inside each *PhysicalFunction.cpp).
  L3 PROVEN — a wired function whose operator is exercised by a systest that
              PASSED. Mapping: systest SQL token (TEMPORAL_…) -> operator NAME
              (PascalCase of the token) -> its meos_call.

Pass the list of green systests (one basename per line, no extension) via
--passing; produce it by running the `systest` binary over
nes-systests/function/meos/ on the accumulated build. Functions called only by
operators with no passing systest come out as `wired` — the honest backlog.
"""
from __future__ import annotations
import argparse
import os
import re
import sys

CALL_RE = re.compile(r"\b([a-z][a-z0-9_]*)\s*\(", )

# A call counts as the operator's backing MEOS function iff it is an actual
# MEOS *streamable* symbol — i.e. it is in the parity surface itself. This is
# measured-not-guessed and family-agnostic: spatial-rels, distance, comparison,
# arithmetic, accessors all qualify automatically, with no per-family regex to
# maintain. Input constructors (tfloat_in, tint_in, tcbuffer_in, …) are NOT in
# the streamable surface (io-meta) so they self-exclude. The two type-conversion
# helpers below ARE streamable but are used here only as composition plumbing
# (e.g. tpose_to_tpoint before a tgeo spatial-rel), so they are not the wired op.
CONVERSION_HELPERS = {"tpose_to_tpoint", "tnpoint_to_tgeompoint"}


def load_streamable(path):
    return {ln.strip() for ln in open(path) if ln.strip()}


def operator_calls(root, streamable):
    """Map operator NAME -> set(meos_call). NAME taken from the file stem
    (…PhysicalFunction.cpp -> the operator name). A call counts iff it is a
    streamable MEOS symbol and not a composition-plumbing conversion."""
    name2calls = {}
    dirs = ["nes-physical-operators/src/Functions/Meos",
            "nes-physical-operators/src/Aggregation/Function/Meos"]
    for d in dirs:
        full = os.path.join(root, d)
        if not os.path.isdir(full):
            continue
        for f in os.listdir(full):
            if not f.endswith("PhysicalFunction.cpp"):
                continue
            name = f[:-len("PhysicalFunction.cpp")]
            txt = open(os.path.join(full, f)).read()
            # find the MEOS backing call: the call assigned to result/returned
            calls = set()
            for m in CALL_RE.finditer(txt):
                fn = m.group(1)
                if fn in streamable and fn not in CONVERSION_HELPERS:
                    calls.add(fn)
            if calls:
                name2calls[name] = calls
    return name2calls


def systest_token(path):
    for line in open(path):
        m = re.search(r"\b(TEMPORAL_[A-Z0-9_]+|[A-Z][A-Z0-9_]*_TGEO_GEO|[A-Z][A-Z0-9_]*_EXTENT|TGEO_AT_STBOX)\s*\(", line)
        if m:
            return m.group(1)
    return None


def build_token2name(root):
    """Authoritative token -> LogicalFunction operator NAME, parsed from the
    parser dispatch (case AntlrSQLLexer::<TOKEN>: … <Name>LogicalFunction(…))."""
    t2n = {}
    parser = os.path.join(root, "nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp")
    if not os.path.exists(parser):
        return t2n
    cur = None
    for line in open(parser):
        mc = re.search(r"case AntlrSQLLexer::([A-Z0-9_]+):", line)
        if mc:
            cur = mc.group(1)
        m = re.search(r"\b([A-Z][A-Za-z0-9]+)LogicalFunction\(", line)
        if m and cur:
            t2n[cur] = m.group(1)
            cur = None
    return t2n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Nebula repo root (accumulated build checkout)")
    ap.add_argument("--systests", default="nes-systests/function/meos")
    ap.add_argument("--passing", help="file: one passing-systest basename per line")
    ap.add_argument("--streamable",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "..", "feeds", "streamable.txt"),
                    help="the streamable MEOS surface (parity universe)")
    a = ap.parse_args()

    streamable = load_streamable(a.streamable)
    name2calls = operator_calls(a.root, streamable)
    token2name = build_token2name(a.root)
    wired = set().union(*name2calls.values()) if name2calls else set()

    passing = set()
    if a.passing and os.path.exists(a.passing):
        passing = {ln.strip() for ln in open(a.passing) if ln.strip()}

    proven = set()
    sysdir = os.path.join(a.root, a.systests)
    if os.path.isdir(sysdir):
        for f in os.listdir(sysdir):
            if not f.endswith(".test"):
                continue
            base = f[:-len(".test")]
            if passing and base not in passing:
                continue
            tok = systest_token(os.path.join(sysdir, f))
            if not tok:
                continue
            name = token2name.get(tok)              # authoritative token -> operator
            if name and name in name2calls:
                proven |= name2calls[name]

    for fn in sorted(wired):
        print(f"{fn}\t{'proven' if fn in proven else 'wired'}")
    sys.stderr.write(f"[nebula adapter] operators={len(name2calls)} wired-calls={len(wired)} "
                     f"proven-calls={len(proven)} passing-systests={len(passing) or 'all'}\n")


if __name__ == "__main__":
    main()
