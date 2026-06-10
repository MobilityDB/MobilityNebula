#!/usr/bin/env python3
"""Family-filter the ANTLR grammar at CMake configure time.

MobilityDB/MEOS lets a build drop a whole type family with -DCBUFFER=0 etc. The
generated MobilityNebula operators are already gated two ways: physical TUs by
per-family CMake subdirs, and the parser-cpp #include + dispatch by `#if <FAMILY>`.
This script closes the last gap — the *grammar* — so an excluded family's lexer
keyword tokens and their `functionName` alternatives are physically absent from
the compiled lexer/parser (the keyword then lexes as a plain IDENTIFIER, which the
guarded dispatch rejects as an unknown function).

It reads the master AntlrSQL.g4, drops every lexer token definition and
functionName alternative whose token name belongs to an OFF family, and writes the
filtered grammar to the build dir for antlr4_generate to consume. With all
families ON (the default) the output is byte-identical to the input.

Usage: filter_grammar.py --in <master.g4> --out <filtered.g4> \
                         --cbuffer 1 --npoint 1 --pose 1 --rgeo 1
"""
import argparse
import re
import sys


def token_family(token: str):
    """The toggleable family of a grammar token, or None for a core token.
    Mirrors tools/codegen/codegen_nebula.py:meos_family (same substring rules)."""
    s = token.lower()
    if "trgeo" in s or "rgeometry" in s:
        return "RGEO"
    if "pose" in s:
        return "POSE"
    if "npoint" in s or "nsegment" in s:
        return "NPOINT"
    if "cbuffer" in s:
        return "CBUFFER"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", required=True)
    ap.add_argument("--out", dest="dst", required=True)
    for fam in ("cbuffer", "npoint", "pose", "rgeo"):
        ap.add_argument(f"--{fam}", type=int, default=1)
    a = ap.parse_args()
    off = {fam for fam in ("CBUFFER", "NPOINT", "POSE", "RGEO")
           if getattr(a, fam.lower()) == 0}

    src = open(a.src).read()
    if not off:                       # all families on: identical passthrough
        open(a.dst, "w").write(src)
        return

    out_lines, dropped_tokens = [], 0
    # A lexer token definition: `TOKEN_NAME: 'literal' | ...;` at column 0.
    tokdef_re = re.compile(r"^([A-Z][A-Z0-9_]*)\s*:")
    for line in src.split("\n"):
        m = tokdef_re.match(line)
        if m and token_family(m.group(1)) in off:
            dropped_tokens += 1
            continue                  # drop the whole lexer token definition
        out_lines.append(line)
    body = "\n".join(out_lines)

    # Filter the single `functionName:` alternation rule, dropping alternatives
    # whose token belongs to an off family (IDENTIFIER and core tokens stay).
    fn_re = re.compile(r"(functionName:\s*)(.*?)(\s*;)", re.DOTALL)
    def _filter_fn(m):
        alts = [x.strip() for x in m.group(2).split("|")]
        kept = [x for x in alts if not (token_family(x) in off)]
        return m.group(1) + " | ".join(kept) + m.group(3)
    body, n = fn_re.subn(_filter_fn, body)
    if n != 1:
        sys.stderr.write(f"filter_grammar: expected 1 functionName rule, found {n}\n")
        sys.exit(1)

    open(a.dst, "w").write(body)
    sys.stderr.write(
        f"filter_grammar: families OFF={sorted(off)}; dropped {dropped_tokens} lexer tokens\n")


if __name__ == "__main__":
    main()
