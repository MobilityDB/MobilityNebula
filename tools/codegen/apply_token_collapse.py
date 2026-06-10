#!/usr/bin/env python3
"""Apply the COLLAPSE rows of naming_regularization_map.tsv.

COLLAPSE_PROMOTE: the genuine-typed survivor operator is renamed to the canonical
  regular token (pure token substitution — handled by apply_token_rename.py logic,
  reused here).
COLLAPSE_REMOVE: the operator is a redundant duplicate (it composes via a symbol
  owned by another operator, or its canonical token already exists). Delete it
  whole: operator .cpp/.hpp (logical+physical / aggregation), the two add_plugin
  CMake lines, the parser glue block, the lexer token line, the functionName
  alternation entry, and any .test that invokes the token.

The genuine MEOS symbol each REMOVE operator credited stays proven via the
surviving PROMOTE'd (or pre-existing) canonical operator; the post-build
zero-removal gate confirms this.

Usage:  tools/codegen/apply_token_collapse.py [--apply]   (default: dry-run)
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAP = ROOT / "tools/streaming_parity/naming_regularization_map.tsv"
G4 = ROOT / "nes-sql-parser/AntlrSQL.g4"
PARSER = ROOT / "nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp"


def rows():
    out = []
    for line in MAP.read_text().splitlines()[1:]:
        legacy, sym, regular, outcome = line.split("\t")
        out.append((legacy, sym, regular, outcome))
    return out


def _norm(s):
    return s.replace("_", "").lower()


def _agg_norm2stem():
    """Operator stems (minus Aggregation suffix) by normalized form — for the
    aggregation tokens that are not dispatched via the per-event parser glue."""
    out = {}
    for d in [ROOT / "nes-physical-operators/src/Functions/Meos",
              ROOT / "nes-physical-operators/src/Aggregation/Function/Meos"]:
        if not d.exists():
            continue
        for cpp in d.glob("*.cpp"):
            for suf in ("AggregationPhysicalFunction.cpp", "PhysicalFunction.cpp"):
                if cpp.name.endswith(suf):
                    stem = cpp.name[:-len(suf)]
                    base = stem[:-len("Aggregation")] if stem.endswith("Aggregation") else stem
                    out[_norm(base)] = stem
                    break
    return out


def stem_for(token, parser_text, norm2stem=None):
    """Operator class stem: from the per-event parser glue's functionBuilder
    emplace_back, else by normalized-name match against the operator files
    (covers windowed-aggregation tokens that use the aggregation glue)."""
    m = re.search(r"AntlrSQLLexer::" + re.escape(token) + r"\s*:", parser_text)
    if m:
        seg = parser_text[m.end():m.end() + 4000]
        nxt = seg.find("\n        case ")
        if nxt > 0:
            seg = seg[:nxt]
        e = re.search(r"functionBuilder\.emplace_back\(\s*([A-Za-z0-9_]+?)LogicalFunction", seg)
        if e:
            return e.group(1)
    return (norm2stem or {}).get(_norm(token))


def remove_glue_block(text, token):
    pat = re.compile(r"[ \t]*/\* BEGIN CODEGEN (?:PARSER|AGGREGATION) GLUE: " +
                     re.escape(token) + r"[^\n]*\*/.*?/\* END CODEGEN (?:PARSER|AGGREGATION) GLUE: " +
                     re.escape(token) + r"[^\n]*\*/\n?", re.S)
    return pat.sub("", text)


def files_for_stem(stem):
    cands = [
        ROOT / f"nes-physical-operators/src/Functions/Meos/{stem}PhysicalFunction.cpp",
        ROOT / f"nes-physical-operators/include/Functions/Meos/{stem}PhysicalFunction.hpp",
        ROOT / f"nes-logical-operators/src/Functions/Meos/{stem}LogicalFunction.cpp",
        ROOT / f"nes-logical-operators/include/Functions/Meos/{stem}LogicalFunction.hpp",
        ROOT / f"nes-physical-operators/src/Aggregation/Function/Meos/{stem}AggregationPhysicalFunction.cpp",
        ROOT / f"nes-physical-operators/include/Aggregation/Function/Meos/{stem}AggregationPhysicalFunction.hpp",
    ]
    return [f for f in cands if f.exists()]


def main():
    apply = "--apply" in sys.argv
    data = rows()
    parser_text = PARSER.read_text()
    _AGG_NORM2STEM = _agg_norm2stem()
    promote = [(l, reg) for l, s, reg, oc in data if oc == "COLLAPSE_PROMOTE"]
    remove = [l for l, s, reg, oc in data if oc == "COLLAPSE_REMOVE"]

    # ---- PROMOTE: reuse the rename pass ----
    import apply_token_rename as ren
    repl = ren.make_replacer(promote)

    # ---- REMOVE: gather every artifact ----
    g4_text = G4.read_text()
    del_files, missing_stem = [], []
    cmake_edits = {}  # path -> set(plugin names to drop)
    glue_tokens, lexer_tokens = [], []
    remove_stems = set()
    test_files = set()
    CMAKES = ["nes-logical-operators/src/Functions/Meos/CMakeLists.txt",
              "nes-physical-operators/src/Functions/Meos/CMakeLists.txt",
              "nes-physical-operators/src/Aggregation/Function/Meos/CMakeLists.txt",
              "nes-logical-operators/src/Operators/Windows/Aggregations/Meos/CMakeLists.txt"]
    for t in remove:
        stem = stem_for(t, parser_text, _AGG_NORM2STEM)
        if not stem:
            missing_stem.append(t)
            continue
        remove_stems.add(stem)
        del_files += files_for_stem(stem)
        for cm in CMAKES:
            cmake_edits.setdefault(cm, set()).add(stem)
        glue_tokens.append(t)
        lexer_tokens.append(t)
    for f in (ROOT / "nes-systests").rglob("*.test"):
        txt = f.read_text()
        if any(re.search(r"(?<![A-Za-z0-9_])" + re.escape(t) + r"(?![A-Za-z0-9_])", txt) for t in remove):
            test_files.add(f)

    print(f"PROMOTE renames: {len(promote)}")
    print(f"REMOVE tokens: {len(remove)}  (no-stem: {len(missing_stem)} -> {missing_stem})")
    print(f"  operator files to delete: {len(set(del_files))}")
    print(f"  .test files to delete: {len(test_files)}")
    print(f"  CMake files to edit: {len([c for c,s in cmake_edits.items() if s])}")

    if not apply:
        print("\n(dry-run; pass --apply to execute)")
        return

    # PROMOTE token renames across grammar/parser/tests/aggregation
    targets = [G4, PARSER] + sorted((ROOT / "nes-systests").rglob("*.test")) \
              + sorted((ROOT / "nes-physical-operators/src/Aggregation").rglob("*.cpp"))
    for f in targets:
        old = f.read_text(); new = repl(old)
        if new != old:
            f.write_text(new)
    # REMOVE: parser glue + grammar + files + cmake + tests
    parser_text = PARSER.read_text()
    for t in glue_tokens:
        parser_text = remove_glue_block(parser_text, t)
    PARSER.write_text(parser_text)
    # remove the optimizer-lowering glue blocks (keyed by operator STEM, not token)
    # that construct each windowed-aggregation operator in the lowering rule.
    lower = ROOT / "nes-query-optimizer/src/RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.cpp"
    if lower.exists():
        ltext = lower.read_text()
        # the optimizer-lowering glue markers are keyed inconsistently — some by
        # operator stem, some by the SQL token — so strip blocks matching either.
        for key in set(remove_stems) | set(remove):
            ltext = re.sub(
                r"[ \t]*/\* BEGIN CODEGEN GLUE: " + re.escape(key) +
                r" \(optimizer lowering\) \*/.*?/\* END CODEGEN GLUE: " +
                re.escape(key) + r" \(optimizer lowering\) \*/\n?", "", ltext, flags=re.S)
        lower.write_text(ltext)
    g4_text = G4.read_text()
    for t in lexer_tokens:
        g4_text = re.sub(r"^" + re.escape(t) + r"\s*:.*\n", "", g4_text, flags=re.M)
        g4_text = re.sub(r"\s*\|\s*" + re.escape(t) + r"(?![A-Za-z0-9_])", "", g4_text)
    G4.write_text(g4_text)
    for f in set(del_files):
        f.unlink()
    for cm, stems in cmake_edits.items():
        p = ROOT / cm
        if not p.exists():
            continue
        lines = p.read_text().splitlines(keepends=True)
        kept = [ln for ln in lines
                if not any(re.search(r"add_plugin\(" + re.escape(s) + r"\b", ln) for s in stems)]
        p.write_text("".join(kept))
    for f in test_files:
        f.unlink()
    # strip dangling #include lines pointing at the deleted operator headers
    # (the parser and the windowed-aggregation lowering rule #include every
    # operator header by name).
    inc_roots = [ROOT / "nes-physical-operators/include", ROOT / "nes-logical-operators/include"]
    inc_rx = re.compile(
        r"^[ \t]*#include\s*<((?:Functions/Meos|Aggregation/Function/Meos|"
        r"Operators/Windows/Aggregations/Meos)/[A-Za-z0-9_]+\.hpp)>[ \t]*\n", re.M)
    consumers = []
    for d in ROOT.glob("nes-*"):
        if d.is_dir():
            consumers += list(d.rglob("*.cpp")) + list(d.rglob("*.hpp"))
    stripped = 0
    for f in consumers:
        txt = f.read_text()

        def drop(m):
            nonlocal stripped
            inc = m.group(1)
            if any((r / inc).exists() for r in inc_roots):
                return m.group(0)        # header still exists -> keep
            stripped += 1
            return ""
        new = inc_rx.sub(drop, txt)
        if new != txt:
            f.write_text(new)
    print(f"\nAPPLIED: removed {len(remove)} operators, promoted {len(promote)} tokens, "
          f"stripped {stripped} dangling includes.")


if __name__ == "__main__":
    main()
