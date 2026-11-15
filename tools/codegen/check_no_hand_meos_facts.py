#!/usr/bin/env python3
"""Check that the generator states no MEOS fact of its own.

The question this answers
-------------------------
A generator accumulates tables. Some are legitimate and some are drift, and by
eye the two are indistinguishable, which is how a map that sent every
`trgeometry` symbol to the wrong header survived beside four correct entries,
and how five whole families ended up with no header at all.

The line is not a matter of taste, and it can be checked. A generator may hold
the facts of its OWN target — how a C type becomes a NebulaStream Nautilus
type, how a temporal instant is decomposed into stream columns — because
nothing upstream knows them. It may not hold a fact the catalog already
states: which header declares a symbol, what parses a type, which functions
belong to a family. Those are MEOS's, the catalog carries them, and a copy can
only go stale.

So the rule is mechanical: a hand-written table in the generator may not be
KEYED BY, or hold, a MEOS type or symbol name. The catalog supplies the
vocabulary to check against, so this needs no list of its own and no
maintenance as MEOS grows.

What it does not check
----------------------
Names appearing inside a docstring, a comment, or a classifier's matching logic
are not tables — a shape that recognises `ever_eq` is reading a name, not
storing a fact about one. Only module-level literal containers are examined.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Tables the generator legitimately owns, each because its content belongs to
# NebulaStream rather than to MEOS. Naming them here rather than pattern-matching
# keeps the exemption reviewable: an entry has to be argued for once, in writing.
OWNED = {
    "_SCALAR_RET": "the Nautilus type enum (FLOAT64/INT32/BOOLEAN) — NebulaStream's own type system, which no upstream catalog describes",
    "SCALAR_CPP": "C type to C++ spelling for a scalar operand — the binding's marshalling, not a MEOS fact",
    "_A_TGEO_GEO": "the stream event schema for a temporal geo instant — how an instant is decomposed into columns",
    "_A_TWO_TGEO": "the stream event schema for two temporal geo instants",
    "_A_TCB_CB": "the stream event schema for a temporal circular-buffer instant",
    "_A_TWO_TCB": "the stream event schema for two temporal circular-buffer instants",
    "SHAPES": "the classifier registry — names of this file's own functions",
    "_STREAMABLE_CATEGORIES": "which catalog categories carry a per-event operator — a projection rule over a catalog field, holding no type name",
    "GENERIC_RETURNS": "C return type to Nautilus marshaller — the `bool` it holds is the C type, not the MEOS base type of the same spelling",
    "SQL_SCALAR": "SQL scalar type to the NebulaStream data type carrying it and the C type of its slot (codegen_nebula_sql.py) — the `bool` it holds is that C type",
    "C_NUMBER": "the C types a number crosses a kernel as (codegen_nebula_sql.py) — the `bool` it holds is the C type, not the MEOS base type",
    "_EXTRACT_KIND": "C type of an extracted value to the marshaller carrying it — the same C-type vocabulary, resolved against the catalog per operator",
    "_GENERIC_INPUT_FOR": (
        "which concrete type represents a CLASS token. The catalog states class "
        "membership in the number and spatial flags of temporalTypes but not which "
        "member represents the class, because that is this emitter's choice: "
        "resolving tspatial to the first buildable member alphabetically gives "
        "tcbuffer, and 58 operators then parse a circular buffer where a geometry "
        "point is meant"
    ),
}

# A table that SHOULD be derived and is not yet. It is named here rather than
# excused in OWNED, so the check passes while saying what is outstanding: an
# entry here is debt with a reason, not a table this generator is entitled to.
DEBT = {
    "GENERIC_INPUTS": (
        "the WKT literal grammar for a COMPOSITE operand -- a geometry, a "
        "circular buffer, a pose, a network point. The scalar-base half of this "
        "table is retired: build_descriptor's scalar_base_input reads "
        "<T>inst_make from the catalog and the operand is constructed rather "
        "than spelled, which also lifted the ceiling to admit tbigint, th3index "
        "and tquadbin. What remains needs the BASE value built first, through "
        "the base type's own catalogued constructor, and that is a second step"
    ),
}


def meos_vocabulary(catalog_path):
    """Every MEOS type and symbol name, from the catalog."""
    data = json.loads(Path(catalog_path).read_text())
    names = {f["name"] for f in data["functions"]}
    types = set(data.get("temporalTypes") or {}) | set(data.get("typeEncodings") or {})
    # the base type each temporal type carries, and the box that bounds it
    for spec in (data.get("temporalTypes") or {}).values():
        for key in ("base", "bbox"):
            if spec.get(key):
                types.add(spec[key])
    return names, {t for t in types if len(t) > 2}


def literals(node):
    """Every string constant anywhere inside an AST node."""
    return [n.value for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def findings(source_path, names, types):
    src = Path(source_path).read_text()
    tree = ast.parse(src)
    out = []
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, (ast.Dict, ast.List, ast.Set, ast.Tuple)):
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id in OWNED or target.id in DEBT:
            continue
        held = set(literals(node.value))
        meos = sorted((held & names) | (held & types))
        if meos:
            out.append((node.lineno, target.id, meos))
    return out


def main() -> int:
    catalog = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "meos-idl.json")
    if not Path(catalog).exists():
        print(f"ERROR: no catalog at {catalog}; pass its path as the first argument.",
              file=sys.stderr)
        return 1
    names, types = meos_vocabulary(catalog)
    if not names or not types:
        print("ERROR: the catalog yielded no vocabulary; it is the wrong file.",
              file=sys.stderr)
        return 1

    failures = 0
    scanned = 0
    for source in sorted(HERE.glob("*.py")):
        if source.name == Path(__file__).name:
            continue
        scanned += 1
        for line, name, meos in findings(source, names, types):
            print(f"{source.name}:{line}: {name} states MEOS facts: {', '.join(meos[:8])}"
                  + (" …" if len(meos) > 8 else ""))
            failures += 1

    if not scanned:
        print("ERROR: scanned no generator sources; the directory is wrong.", file=sys.stderr)
        return 1
    if failures:
        print(f"\nERROR: {failures} hand-written table(s) hold a MEOS name.", file=sys.stderr)
        print("Read it from the catalog instead — it carries the declaring header "
              "(functions[].file), the parser and serializer per type "
              "(typeEncodings), and the base type and class flags per temporal "
              "type (temporalTypes).", file=sys.stderr)
        print("A table that genuinely belongs to NebulaStream rather than to MEOS "
              "goes in OWNED with the reason it does.", file=sys.stderr)
        return 1
    print(f"OK: no generator table states a MEOS fact "
          f"({scanned} source(s), {len(names)} symbols and {len(types)} types checked).")
    for name, why in sorted(DEBT.items()):
        print(f"   outstanding: {name} — {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
