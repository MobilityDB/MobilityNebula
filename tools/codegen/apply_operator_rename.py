#!/usr/bin/env python3
"""Rename legacy operator classes/files to Camel(regular_token).

Completes the RENAME/PROMOTE token migration: an operator's class name, files,
CMake add_plugin name, and parser-glue references must all derive from its
genuine MEOS symbol (class == Camel(token) == Camel(symbol.upper())), the same
shape codegen emits. This keeps the binding fully catalog-derivable and lets the
streaming-parity adapter map token<->operator by name.

Resolution: for every operator .cpp under Functions/Meos and Aggregation/.../Meos,
read its genuine meos_call (last streamable call that is not a type converter).
If that symbol is a RENAME/PROMOTE target, the operator's regular class stem is
Camel(regular_token); rename the class everywhere if it differs.

Usage: tools/codegen/apply_operator_rename.py [--apply]   (default dry-run)
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAP = ROOT / "tools/streaming_parity/naming_regularization_map.tsv"
SYMS = set((ROOT / "tools/streaming_parity/meos_exported_symbols.txt").read_text().split())
CONV = {"tnpoint_to_tgeompoint", "tpose_to_tpoint", "tnpoint_in", "tpose_in", "tcbuffer_in",
        "tgeompoint_in", "tgeometry_in", "geom_in", "geog_in", "tint_in", "tfloat_in",
        "tbool_in", "ttext_in", "temporal_in", "tgeompointinst_make", "tgeogpoint_in",
        "cbuffer_in", "geometry_in"}


def camel(token):
    return "".join(p.capitalize() for p in token.split("_"))


def primary_call(cpp_text):
    calls = [c for c in re.findall(r"\b([a-z][a-z0-9_]+)\s*\(", cpp_text) if c in SYMS]
    main = [c for c in calls if c not in CONV]
    return (main[-1] if main else (calls[-1] if calls else None))


def main():
    apply = "--apply" in sys.argv

    def norm(s):
        return s.replace("_", "").lower()

    # Map every operator stem (minus Aggregation suffix) by its normalized form,
    # exactly as the streaming-parity adapter does, so we can match an operator
    # to a legacy token 1:1 (no primary-call guessing — composition operators
    # share the genuine symbol and would collide).
    norm2stem = {}
    op_dirs = [ROOT / "nes-physical-operators/src/Functions/Meos",
               ROOT / "nes-physical-operators/src/Aggregation/Function/Meos"]
    for d in op_dirs:
        if not d.exists():
            continue
        for cpp in d.glob("*.cpp"):
            for suf in ("AggregationPhysicalFunction.cpp", "PhysicalFunction.cpp"):
                if cpp.name.endswith(suf):
                    stem = cpp.name[:-len(suf)]
                    base = stem[:-len("Aggregation")] if stem.endswith("Aggregation") else stem
                    norm2stem[norm(base)] = stem
                    break

    # RENAME rows only: their tokens are already migrated in the grammar. The
    # operator to rename is the one whose current stem matches the LEGACY token.
    pairs = {}   # old_stem -> new_stem
    import os
    outcomes = set(os.environ.get("OUTCOMES", "RENAME").split(","))
    for line in MAP.read_text().splitlines()[1:]:
        legacy, sym, regular, outcome = line.split("\t")
        if outcome not in outcomes:
            continue
        old_stem = norm2stem.get(norm(legacy))
        if not old_stem:
            continue
        new_stem = camel(regular)
        if new_stem != old_stem:
            pairs[old_stem] = new_stem
    print(f"operator class renames: {len(pairs)}")
    for o, n in sorted(pairs.items()):
        print(f"  {o:42} -> {n}")
    if not apply:
        print("\n(dry-run; pass --apply)")
        return

    # 1) rename files (any class shape) for each stem, in every operator dir
    class_suffixes = ["LogicalFunction", "PhysicalFunction",
                      "AggregationPhysicalFunction", "AggregationLogicalFunction"]
    file_suffixes = [s + e for s in class_suffixes for e in (".cpp", ".hpp")]
    search_dirs = []
    for d in ["nes-physical-operators", "nes-logical-operators"]:
        for sub in ["src/Functions/Meos", "include/Functions/Meos",
                    "src/Aggregation/Function/Meos", "include/Aggregation/Function/Meos",
                    "src/Operators/Windows/Aggregations/Meos",
                    "include/Operators/Windows/Aggregations/Meos"]:
            search_dirs.append(ROOT / d / sub)
    for old, new in pairs.items():
        for d in search_dirs:
            for suf in file_suffixes:
                f = d / f"{old}{suf}"
                if f.exists():
                    f.rename(d / f"{new}{suf}")
    # 2) replace the full class identifiers ({stem}{suffix}) and the bare stem
    #    (add_plugin first arg) across all source + CMake. Full-identifier keys
    #    are required because the bare stem is a PREFIX of the class name, so an
    #    identifier-boundary match on the bare stem alone would miss it.
    repl = {}
    for old, new in pairs.items():
        for suf in class_suffixes:
            repl[old + suf] = new + suf
            # plugin self-registration fn names embed the stem after "Register"
            repl["Register" + old + suf] = "Register" + new + suf
        repl[old] = new  # bare: CMake add_plugin name, standalone refs
    rx = re.compile(r"(?<![A-Za-z0-9_])(" +
                    "|".join(re.escape(k) for k in sorted(repl, key=len, reverse=True)) +
                    r")(?![A-Za-z0-9_])")
    # Scan every nes-* source dir: operator headers are also #included by
    # consumers outside the operator libs (e.g. nes-query-optimizer's
    # windowed-aggregation lowering rule references each MEOS aggregation class).
    files = []
    for d in sorted(ROOT.glob("nes-*")):
        if not d.is_dir():
            continue
        for ext in ("*.cpp", "*.hpp", "*.h"):
            files += d.rglob(ext)
        files += d.rglob("CMakeLists.txt")
    for f in files:
        t = f.read_text()
        n = rx.sub(lambda m: repl[m.group(1)], t)
        if n != t:
            f.write_text(n)
    print(f"\nAPPLIED: renamed {len(pairs)} operator classes.")


if __name__ == "__main__":
    main()
