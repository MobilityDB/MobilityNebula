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
# helpers below ARE streamable: they are composition plumbing (and not the wired
# op) ONLY when they co-occur with another streamable call in the same operator
# (e.g. tpose_to_tpoint before a tgeo spatial-rel). When such a helper is an
# operator's SOLE streamable call, the operator is a dedicated conversion (e.g.
# TNPOINT_TO_TGEOMPOINT_EXP materializing a network-resolved trajectory) and the
# helper IS its wired op — see operator_calls().
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
            helpers = set()
            for m in CALL_RE.finditer(txt):
                fn = m.group(1)
                if fn not in streamable:
                    continue
                (helpers if fn in CONVERSION_HELPERS else calls).add(fn)
            # A conversion helper is plumbing and is dropped, EXCEPT when the
            # operator is named for it (a dedicated conversion operator, e.g.
            # TnpointToTgeompointExp -> tnpoint_to_tgeompoint): there the helper
            # IS the operator's headline op. (The expand substrate's accumulator
            # calls temporal_append_tinstant/temporal_merge co-occur in every
            # aggregate, so co-occurrence alone cannot distinguish the two.)
            snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
            for h in helpers:
                if snake == h or snake.startswith(h + "_"):
                    calls.add(h)
            if calls:
                name2calls[name] = calls
    return name2calls


def _all_tokens(path):
    """Every UPPER_SNAKE identifier called (followed by `(`) in a systest — the
    candidate SQL function tokens the test exercises, in file order."""
    out = []
    for line in open(path):
        for m in re.finditer(r"\b([A-Z][A-Z0-9_]{2,})\s*\(", line):
            out.append(m.group(1))
    return out


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
    wired = set().union(*name2calls.values()) if name2calls else set()

    # Map a systest's SQL token to its operator by normalizing both to
    # underscore-free lowercase (TLENGTH_EXP -> tlengthexp; the operator key
    # TLengthExpAggregation, minus its Aggregation suffix, -> tlengthexp). This
    # covers every family (per-event, …_EXP, …_WKB, …_EXTENT/UNION, TNPOINT_…)
    # without a per-family pattern — the parser-dispatch token map only captures
    # per-event functions, not windowed aggregates.
    def _norm(s):
        return s.replace("_", "").lower()
    norm2name = {}
    for k in name2calls:
        base = k[:-len("Aggregation")] if k.endswith("Aggregation") else k
        norm2name[_norm(base)] = k

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
            # The first called UPPER_SNAKE token that resolves to an operator.
            for cand in _all_tokens(os.path.join(sysdir, f)):
                name = norm2name.get(_norm(cand))
                if name:
                    proven |= name2calls[name]
                    break

    for fn in sorted(wired):
        print(f"{fn}\t{'proven' if fn in proven else 'wired'}")
    sys.stderr.write(f"[nebula adapter] operators={len(name2calls)} wired-calls={len(wired)} "
                     f"proven-calls={len(proven)} passing-systests={len(passing) or 'all'}\n")


if __name__ == "__main__":
    main()
