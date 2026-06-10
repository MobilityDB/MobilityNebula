#!/usr/bin/env python3
"""Apply the RENAME rows of naming_regularization_map.tsv to the source tree.

A token rename is a pure identifier substitution of the OLD token (and its
lowercase literal) by the regular token across:
  - nes-sql-parser/AntlrSQL.g4            (lexer def line + functionName entry)
  - nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp  (case labels, markers, msgs)
  - nes-systests/**/*.test                (the SQL invocations)

Single-pass dict replacement on identifier boundaries, so a freshly-written NEW
token is never re-matched as another OLD. RENAME targets are collision-free by
construction (the map only classifies a token RENAME when no existing regular
token owns its target).
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAP = ROOT / "tools/streaming_parity/naming_regularization_map.tsv"


def rename_pairs():
    import os
    outcomes = set(os.environ.get("OUTCOMES", "RENAME").split(","))
    pairs = []
    for line in MAP.read_text().splitlines()[1:]:
        legacy, sym, regular, outcome = line.split("\t")
        if outcome in outcomes:
            pairs.append((legacy, regular))
    return pairs


def make_replacer(pairs):
    upper = {o: n for o, n in pairs}
    lower = {o.lower(): n.lower() for o, n in pairs}
    both = {**upper, **lower}
    # longest-first so no key is a prefix issue; identifier boundaries anyway
    rx = re.compile(r"(?<![A-Za-z0-9_])(" +
                    "|".join(re.escape(k) for k in sorted(both, key=len, reverse=True)) +
                    r")(?![A-Za-z0-9_])")
    return lambda text: rx.sub(lambda m: both[m.group(1)], text)


def main():
    pairs = rename_pairs()
    repl = make_replacer(pairs)
    targets = [ROOT / "nes-sql-parser/AntlrSQL.g4",
               ROOT / "nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp"]
    targets += sorted((ROOT / "nes-systests").rglob("*.test"))
    # aggregation operators dispatch the *_EXP / *_EXTENT / *_UNION tokens; their
    # source carries the token in error messages / funcName guards too.
    targets += sorted((ROOT / "nes-physical-operators/src/Aggregation").rglob("*.cpp"))
    changed = 0
    for f in targets:
        old = f.read_text()
        new = repl(old)
        if new != old:
            f.write_text(new)
            changed += 1
    print(f"RENAME pairs: {len(pairs)}; files changed: {changed}")


if __name__ == "__main__":
    main()
