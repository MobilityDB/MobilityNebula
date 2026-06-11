#!/usr/bin/env python3
"""Build a codegen_nebula.py descriptor from classified MEOS gap functions.

Reads a header-signature dump (one `extern <ret> <name>(<args>);` per line, as
produced from the in-container /usr/local/include/meos*.h) plus a gap list
(streamable functions not yet wired by a Nebula operator), classifies each
function into a known operator SHAPE, and emits the JSON descriptor that
codegen_nebula.py consumes.

A SHAPE knows: the per-event SQL arg list, the `build_*` flag selecting the
physical-cpp template, the MEOS return marshaling, and the per-base-type input
ctor (tfloat_in / tint_in / ...). Adding a family = adding a classifier here;
the C++ templates live in codegen_nebula.py.

This keeps the bulk-emission measured-not-guessed: only functions whose exact
in-header signature matches a SHAPE are emitted, and only if they are in the
gap. Functions that don't match any SHAPE are reported, never silently dropped.

Usage:
  build_descriptor.py --sigs /tmp/cmp_sigs.txt --gap /tmp/nebula_gap.txt \\
                      --shapes cmp_scalar_tempfirst --out wave22.json
"""
import argparse
import json
import re
import sys

# --- per-base-type input construction -------------------------------------
# token in the meos-call name -> (tnumber_in_fn, cpp value type, wkt format)
BASE = {
    "tfloat": ("tfloat_in", "double", "{}@{}"),
    "tint":   ("tint_in",   "int",    "{}@{}"),
}
SCALAR_CPP = {"double": "double", "int": "int"}


def pascal(meos_call):
    return "".join(p.capitalize() for p in meos_call.split("_"))


def parse_sigs(path):
    """name -> (ret, [normalized-arg-type, ...])."""
    out = {}
    for ln in open(path):
        m = re.match(r"extern\s+([\w ]+?)\s*(\*?)\s*(\w+)\(([^)]*)\);", ln.strip())
        if not m:
            continue
        ret = (m.group(1) + m.group(2)).strip()
        name = m.group(3)
        args = []
        for a in m.group(4).split(","):
            a = a.strip()
            if not a or a == "void":
                continue
            base = re.sub(r"^const\s+", "", a)
            ty = base.split()[0] + ("*" if "*" in a else "")
            args.append(ty)
        out[name] = (ret, tuple(args))
    return out


# --- SHAPE classifiers ------------------------------------------------------
# Each returns a descriptor dict for `fn` if it matches, else None.

def cmp_scalar_tempfirst(fn, ret, args):
    """ever/always comparison: int fn(const Temporal*, double|int), temp first.
    Reuses build_tnumber_point_with_scalar (already in codegen_nebula.py)."""
    if ret != "int" or args not in (("Temporal*", "double"), ("Temporal*", "int")):
        return None
    base = "tfloat" if args[1] == "double" else "tint"
    in_fn, vcpp, wkt = BASE[base]
    return {
        "nebula_name": pascal(fn),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "args": [
            {"name": "value", "nautilus_type": vcpp, "cpp_type": vcpp},
            {"name": "timestamp", "nautilus_type": "uint64_t", "cpp_type": "uint64_t"},
            {"name": "scalar", "nautilus_type": vcpp, "cpp_type": vcpp},
        ],
        "return_type": "int",
        "nautilus_return": "INT32",
        "build_tnumber_point_with_scalar": True,
        "tnumber_value_cpp_type": vcpp,
        "scalar_cpp_type": SCALAR_CPP[vcpp],
        "tnumber_wkt_format": wkt,
        "tnumber_in_fn": in_fn,
        "comment_one_liner": (
            f"Per-event {fn.split('_')[0]} comparison of a single-instant {base} "
            f"(built from value+timestamp) against a scalar constant."),
    }


def cmp_scalar_scalarfirst(fn, ret, args):
    """ever/always comparison: int fn(double|int, const Temporal*), scalar first.
    Reuses build_tnumber_scalar_first (MEOS call passes scalar as 1st arg)."""
    if ret != "int" or args not in (("double", "Temporal*"), ("int", "Temporal*")):
        return None
    base = "tfloat" if args[0] == "double" else "tint"
    in_fn, vcpp, wkt = BASE[base]
    return {
        "nebula_name": pascal(fn),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "args": [
            {"name": "value", "nautilus_type": vcpp, "cpp_type": vcpp},
            {"name": "timestamp", "nautilus_type": "uint64_t", "cpp_type": "uint64_t"},
            {"name": "scalar", "nautilus_type": vcpp, "cpp_type": vcpp},
        ],
        "return_type": "int",
        "nautilus_return": "INT32",
        "build_tnumber_scalar_first": True,
        "tnumber_value_cpp_type": vcpp,
        "scalar_cpp_type": SCALAR_CPP[vcpp],
        "tnumber_wkt_format": wkt,
        "tnumber_in_fn": in_fn,
        "comment_one_liner": (
            f"Per-event {fn.split('_')[0]} comparison of a scalar constant against a "
            f"single-instant {base} (built from value+timestamp); scalar-first MEOS arg order."),
    }


def cmp_two_temporal(fn, ret, args):
    """Generic two-temporal comparison: int fn(const Temporal*, const Temporal*)
    on the *_temporal_temporal functions. Builds two single-instant tfloats from
    (valueA,tsA)/(valueB,tsB) and reuses build_two_tnumber_points."""
    if ret != "int" or args != ("Temporal*", "Temporal*") or not fn.endswith("_temporal_temporal"):
        return None
    in_fn, vcpp, wkt = BASE["tfloat"]
    return {
        "nebula_name": pascal(fn),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "args": [
            {"name": "valueA", "nautilus_type": vcpp, "cpp_type": vcpp},
            {"name": "tsA", "nautilus_type": "uint64_t", "cpp_type": "uint64_t"},
            {"name": "valueB", "nautilus_type": vcpp, "cpp_type": vcpp},
            {"name": "tsB", "nautilus_type": "uint64_t", "cpp_type": "uint64_t"},
        ],
        "return_type": "int",
        "nautilus_return": "INT32",
        "build_two_tnumber_points": True,
        "tnumber_value_cpp_type": vcpp,
        "tnumber_wkt_format": wkt,
        "tnumber_in_fn": in_fn,
        "comment_one_liner": (
            f"Per-event {fn.split('_')[0]} comparison between two single-instant "
            f"temporals (built from valueA/tsA and valueB/tsB)."),
    }


def _args(spec):
    return [{"name": n, "nautilus_type": t,
             "cpp_type": "const char*" if t == "VariableSizedData" else t} for n, t in spec]


# per-template SQL arg layouts (must match the physical-cpp template's parameterValues order)
_A_TGEO_GEO = [("lon", "double"), ("lat", "double"), ("timestamp", "uint64_t"), ("geometry", "VariableSizedData")]
_A_TWO_TGEO = [("lonA", "double"), ("latA", "double"), ("tsA", "uint64_t"),
               ("lonB", "double"), ("latB", "double"), ("tsB", "uint64_t")]
_A_TCB_CB   = [("lon", "double"), ("lat", "double"), ("radius", "double"), ("timestamp", "uint64_t"), ("cbuffer", "VariableSizedData")]
_A_TWO_TCB  = [("lonA", "double"), ("latA", "double"), ("radiusA", "double"), ("tsA", "uint64_t"),
               ("lonB", "double"), ("latB", "double"), ("radiusB", "double"), ("tsB", "uint64_t")]


def _mk_int(fn, flag, argspec, note):
    return {
        "nebula_name": pascal(fn),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "args": _args(argspec),
        "return_type": "int",
        "nautilus_return": "INT32",
        flag: True,
        "comment_one_liner": note,
    }


def sprel_cmp_existing(fn, ret, args):
    """int-returning spatial-relation / comparison whose per-event input build is
    already covered by an existing physical-cpp template. Routes by arg-shape +
    type token; returns None for shapes needing a new template (tnpoint/tpose
    native, reversed-arg, text/bool, non-int return)."""
    if ret != "int":
        return None
    if args == ("Temporal*", "GSERIALIZED*") and ("_tgeo_geo" in fn or "_tpoint_geo" in fn):
        return _mk_int(fn, "build_temporal_point", _A_TGEO_GEO,
                       f"Per-event {fn} between a single-instant tgeompoint and a static geometry.")
    if args == ("Temporal*", "Temporal*") and "tcbuffer" in fn:
        return _mk_int(fn, "build_two_tcbuffer_points", _A_TWO_TCB,
                       f"Per-event {fn} between two single-instant tcbuffers.")
    if args == ("Temporal*", "Temporal*") and not ("tnpoint" in fn or "tpose" in fn):
        return _mk_int(fn, "build_two_temporal_points", _A_TWO_TGEO,
                       f"Per-event {fn} between two single-instant tgeompoints.")
    if args == ("Temporal*", "Cbuffer*"):
        return _mk_int(fn, "build_tcbuffer_point_cbuffer", _A_TCB_CB,
                       f"Per-event {fn} between a single-instant tcbuffer and a static cbuffer.")
    return None


SHAPES = {
    "cmp_scalar_tempfirst": cmp_scalar_tempfirst,
    "cmp_scalar_scalarfirst": cmp_scalar_scalarfirst,
    "cmp_two_temporal": cmp_two_temporal,
    "sprel_cmp_existing": sprel_cmp_existing,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sigs", required=True, help="header signature dump")
    ap.add_argument("--gap", required=True, help="streamable-not-wired function list")
    ap.add_argument("--shapes", required=True,
                    help="comma-separated SHAPE names to apply (order = match priority)")
    ap.add_argument("--out", required=True, help="descriptor JSON output path")
    a = ap.parse_args()

    sigs = parse_sigs(a.sigs)
    gap = {ln.strip() for ln in open(a.gap) if ln.strip()}
    shapes = [SHAPES[s] for s in a.shapes.split(",")]

    ops, unmatched = [], []
    for fn in sorted(gap):
        if fn not in sigs:
            continue
        ret, args = sigs[fn]
        for cls in shapes:
            d = cls(fn, ret, args)
            if d:
                ops.append(d)
                break
        else:
            unmatched.append(fn)

    json.dump({"_comment": f"codegen descriptor; shapes={a.shapes}", "operators": ops},
              open(a.out, "w"), indent=2)
    sys.stderr.write(f"emitted {len(ops)} operator descriptor(s) -> {a.out}\n")
    sys.stderr.write(f"(gap functions present in sig-dump but unmatched by these shapes: "
                     f"{len([f for f in unmatched if f in sigs])})\n")


if __name__ == "__main__":
    main()
