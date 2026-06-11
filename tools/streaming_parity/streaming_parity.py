#!/usr/bin/env python3
"""Streaming-parity harness — PROVEN (measured, not guessed) coverage of the
MEOS function surface by a streaming platform (Flink / Kafka / NebulaStream).

This is the streaming-platform sibling of MobilityDB's cross-type parity-audit
harness (`tools/parity_audit/`, MobilityDB #1110). Same discipline, different
axis:

  * parity_audit  : does each temporal TYPE cover its family-reference's ops?
  * streaming_parity: does each PLATFORM cover the *streamable* MEOS surface?

THE FIX FOR "GUESSED 100%": a function is covered only when a test PASSES — not
because a generic wiring class *could* wrap it, and not because an operator is
registered. Coverage is measured at three layers (deepest reached wins):

  L1 EXPORTED   — the MEOS symbol is exported by the pinned libmeos (`nm -D`).
  L2 WIRED      — the platform registers an operator/UDF that calls it
                  (Nebula: operator's `meos_call` + SQL token; Flink/Kafka:
                  a generated facade method — javap/reflection).
  L3 PROVEN     — a test exercising that operator/method passes green
                  (Nebula: a systest; Flink/Kafka: a JUnit).

Only L3 counts toward TRUE parity. L2-only is reported separately as
"wired, unproven" so a future session knows exactly what still needs a test.

NON-STREAMABLE tiers are reason-marked exclusions (the streaming sibling of the
semantic/structural exclusions in cross_type_parity.md) and never count as
gaps:
  * io-meta       — I/O / catalog / infra, not a domain operator.
  * sequence-only — needs a whole completed sequence, not a per-event handle.
  * internal      — not part of the public API.
  * ambiguous     — open design question (the formal streamingSemantics RFC).

Run against an ACCUMULATED-PR build of the platform (all open PRs merged), so
parity is measured against the full *intended* surface, not stale master.

Usage:
  streaming_parity.py --catalog streaming-relevance-baseline.json \
                      --platform Nebula --feed nebula.feed.tsv [--baseline old.feed.tsv]

  feed.tsv rows:  <meos_function><TAB>{proven|wired}
"""
from __future__ import annotations
import argparse
import json
from collections import Counter

STREAMABLE = {"stateless", "bounded-state", "windowed", "cross-stream"}
# Reason-marked NON-streamable tiers: never gaps, never "implement to close".
NON_STREAMABLE_REASON = {
    "io-meta":       "I/O / catalog / infrastructure — not a streaming domain operator",
    "sequence-only": "needs a whole completed sequence (not a per-event handle); "
                     "only reachable as a windowed closure-of-stream, tracked separately",
    "internal":      "not part of the public MEOS API",
    "ambiguous":     "open streaming-semantics design question (formal streamingSemantics RFC)",
}


def load_catalog(path):
    """function name -> tier, restricted to public API (drop 'internal')."""
    doc = json.load(open(path))
    rows = doc["functions"] if isinstance(doc, dict) else doc
    return {r["name"]: r.get("tier", "internal") for r in rows}


def load_feed(path):
    """function name -> depth ('proven' | 'wired'); 'proven' wins on dup."""
    feed = {}
    for line in open(path):
        p = line.rstrip("\n").split("\t")
        if len(p) >= 2 and p[0]:
            depth = p[1].strip().lower()
            if depth not in ("proven", "wired"):
                continue
            if feed.get(p[0]) != "proven":
                feed[p[0]] = depth
    return feed


def measure(catalog, feed):
    streamable = {n for n, t in catalog.items() if t in STREAMABLE}
    proven = {n for n in streamable if feed.get(n) == "proven"}
    wired = {n for n in streamable if feed.get(n) == "wired"}
    gaps = streamable - proven - wired
    # feed entries that aren't in the streamable catalog (extra / misnamed)
    unknown = {n for n, d in feed.items() if n not in streamable}
    return dict(streamable=streamable, proven=proven, wired=wired, gaps=gaps,
                unknown=unknown)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="streaming-relevance-baseline.json")
    ap.add_argument("--platform", required=True)
    ap.add_argument("--feed", required=True, help="TSV: function<TAB>{proven|wired}")
    ap.add_argument("--baseline", help="prior feed for a Δ column")
    a = ap.parse_args()

    catalog = load_catalog(a.catalog)
    m = measure(catalog, load_feed(a.feed))
    base = measure(catalog, load_feed(a.baseline)) if a.baseline else None
    n_str = len(m["streamable"])

    def pct(x):
        return 100.0 * x / n_str if n_str else 0.0
    nprov, nwire, ngap = len(m["proven"]), len(m["wired"]), len(m["gaps"])
    bcol = " Δ callable |" if base else ""
    print(f"# Streaming parity (CALLABLE, measured) — {a.platform}\n")
    print(f"Streamable MEOS surface (public, tiers {sorted(STREAMABLE)}): **{n_str}**\n")
    print("| layer | meaning | count | % of streamable |" + bcol)
    print("|---|---|---|---|" + ("---|" if base else ""))
    d = f" {pct(nprov) - pct(len(base['proven'])):+.1f} |" if base else ""
    print(f"| **L3 CALLABLE** | binding invokes it on real libmeos | "
          f"**{nprov}** | **{pct(nprov):.1f}%** |{d}")
    print(f"| L2 wired-only | registered, not yet confirmed callable | "
          f"{nwire} | {pct(nwire):.1f}% |" + (" |" if base else ""))
    print(f"| gap | streamable, not wired | {ngap} | {pct(ngap):.1f}% |"
          + (" |" if base else ""))

    print("\n## Non-streamable (reason-marked — NOT gaps, never implement to 'close')\n")
    tally = Counter(t for t in catalog.values())
    for tier, reason in NON_STREAMABLE_REASON.items():
        if tier == "internal":
            continue
        print(f"- **{tier}** ({tally.get(tier, 0)}): {reason}")

    print("\n## L2 wired but UNCONFIRMED — needs a callability test to count (honest backlog)\n")
    wired = sorted(m["wired"])
    head = ", ".join(wired[:40]) + (" …" if len(wired) > 40 else "")
    print(f"{len(wired)} functions: " + head if wired else "(none)")

    print("\n## Real gaps — streamable, not wired on this platform (implement)\n")
    for t in sorted(STREAMABLE):
        gs = sorted(n for n in m["gaps"] if catalog[n] == t)
        tail = (": " + ", ".join(gs[:20]) + (" …" if len(gs) > 20 else "")) if gs else ""
        print(f"- **{t}** ({len(gs)})" + tail)
    if m["unknown"]:
        print(f"\n_note: {len(m['unknown'])} feed entries not in the streamable catalog "
              f"(internal/io-meta/renamed): {', '.join(sorted(m['unknown'])[:15])}…_")


if __name__ == "__main__":
    main()
