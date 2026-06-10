#!/usr/bin/env python3
"""Lint: every Nebula SQL function token must be a genuine MEOS symbol, uppercased.

THE RULE (see naming_regularization.md): token == meos_call.upper().  This keeps
the binding generatable from the MEOS-API catalog with no per-function naming, so
a new MEOS function/type rolls out mechanically.

A token PASSES iff token.lower() is an exported MEOS symbol (nm -D), or it is in
the documented allowlist (SQL builtins + BerlinMOD-benchmark helpers with no 1:1
MEOS symbol).  Exits non-zero (CI gate) if any irregular token remains.

Usage:
  lint_token_naming.py [--grammar nes-sql-parser/AntlrSQL.g4]
                       [--symbols tools/streaming_parity/meos_exported_symbols.txt]
"""
import argparse
import re
import sys
from pathlib import Path

# SQL builtins / Nebula aggregates that are not MEOS symbols.
BUILTINS = {"AVG", "MAX", "MIN", "SUM", "COUNT", "MEDIAN", "ARRAY_AGG", "VAR"}
# BerlinMOD-benchmark helpers with no 1:1 MEOS symbol (documented exceptions).
# TEMPORAL_LENGTH is the windowed trajectory-length aggregation (the per-event
# tpoint_length is the regular TPOINT_LENGTH operator).
BENCHMARK = {"PAIR_MEETING", "TEMPORAL_SEQUENCE", "TEMPORAL_LENGTH", "CROSS_DISTANCE"}
ALLOWLIST = BUILTINS | BENCHMARK


def grammar_function_tokens(grammar: Path):
    """Return the token names listed in the functionName alternation rule."""
    text = grammar.read_text()
    m = re.search(r"^functionName\s*:(.*?);", text, re.S | re.M)
    if not m:
        sys.exit(f"ERROR: no functionName rule found in {grammar}")
    toks = re.findall(r"[A-Z][A-Z0-9_]+", m.group(1))
    return sorted(set(t for t in toks if t != "IDENTIFIER"))


def main():
    ap = argparse.ArgumentParser()
    here = Path(__file__).resolve().parent
    ap.add_argument("--grammar", default=str(here.parent.parent / "nes-sql-parser/AntlrSQL.g4"))
    ap.add_argument("--symbols", default=str(here / "meos_exported_symbols.txt"))
    args = ap.parse_args()

    symbols = set(Path(args.symbols).read_text().split())
    tokens = grammar_function_tokens(Path(args.grammar))

    irregular = []
    for t in tokens:
        if t in ALLOWLIST:
            continue
        if t.lower() not in symbols:
            irregular.append(t)

    regular = len(tokens) - len(irregular) - sum(1 for t in tokens if t in ALLOWLIST)
    print(f"function tokens: {len(tokens)}  regular: {regular}  "
          f"allowlisted: {sum(1 for t in tokens if t in ALLOWLIST)}  "
          f"irregular: {len(irregular)}")
    if irregular:
        print("\nIRREGULAR tokens (token.lower() is not an exported MEOS symbol):")
        for t in irregular:
            print(f"  {t}")
        print("\nFix: rename to meos_call.upper(), or collapse the redundant "
              "composition operator. See naming_regularization.md + "
              "naming_regularization_map.tsv.")
        return 1
    print("OK: all function tokens are regular (token == meos symbol, uppercased).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
