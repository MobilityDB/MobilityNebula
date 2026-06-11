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


_SCALAR_RET = {"int": ("int", "INT32"), "double": ("double", "FLOAT64"), "bool": ("bool", "BOOLEAN")}


def _mk_scalar(fn, flag, argspec, note, ret="int"):
    rt, nr = _SCALAR_RET[ret]
    return {
        "nebula_name": pascal(fn),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "args": _args(argspec),
        "return_type": rt,
        "nautilus_return": nr,
        flag: True,
        "comment_one_liner": note,
    }


def sprel_scalar_existing(fn, ret, args):
    """Scalar-returning (int/double/bool) spatial-relation / comparison / topological /
    distance whose per-event input build is already covered by an existing
    physical-cpp template. Routes by arg-shape + type token; returns None for
    shapes needing a new template (tnpoint/tpose native two-temporal, reversed-arg,
    text/bool object args, non-scalar return)."""
    if ret not in _SCALAR_RET:
        return None
    if args == ("Temporal*", "GSERIALIZED*") and ("_tgeo_" in fn or "_tpoint_" in fn or "_tspatial_" in fn):
        return _mk_scalar(fn, "build_temporal_point", _A_TGEO_GEO,
                          f"Per-event {fn}: single-instant tgeompoint vs a static geometry -> {ret}.", ret)
    if args == ("Temporal*", "Temporal*") and "tcbuffer" in fn:
        return _mk_scalar(fn, "build_two_tcbuffer_points", _A_TWO_TCB,
                          f"Per-event {fn} between two single-instant tcbuffers -> {ret}.", ret)
    if args == ("Temporal*", "Temporal*") and not ("tnpoint" in fn or "tpose" in fn or "tnumber" in fn):
        return _mk_scalar(fn, "build_two_temporal_points", _A_TWO_TGEO,
                          f"Per-event {fn} between two single-instant tgeompoints -> {ret}.", ret)
    if args == ("Temporal*", "Cbuffer*"):
        return _mk_scalar(fn, "build_tcbuffer_point_cbuffer", _A_TCB_CB,
                          f"Per-event {fn}: single-instant tcbuffer vs a static cbuffer -> {ret}.", ret)
    return None


# leading/embedded type token -> generic input-builder key (codegen_nebula GENERIC_INPUTS)
_GENERIC_INPUT_FOR = [
    ("tgeompoint", "tgeompoint"), ("tgeogpoint", "tgeompoint"), ("tgeometry", "tgeometry"),
    ("tcbuffer", "tcbuffer"), ("tnpoint", "tnpoint"), ("tpose", "tpose"),
    ("tfloat", "tfloat"), ("tint", "tint"), ("tbool", "tbool"),
    ("tnumber", "tfloat"), ("tgeo", "tgeompoint"), ("tpoint", "tgeompoint"),
]


def _infer_input(fn):
    for tok, inp in _GENERIC_INPUT_FOR:
        if fn.startswith(tok + "_") or ("_" + tok + "_") in fn or fn.endswith("_" + tok):
            return inp
    return None


def temporal_unary_scalar(fn, ret, args):
    """Unary scalar accessor: int|double|bool fn(const Temporal*). Generic shape;
    input type inferred from the name token."""
    if args != ("Temporal*",):
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    if not rk:
        return None
    inp = _infer_input(fn)
    if not inp:
        return None
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp, "extra_args": [], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: {rk} accessor over a single-instant {inp}.",
    }


_SCALAR_CPP = {"double": "double", "int": "int32_t", "bool": "bool"}


def temporal_x_scalar(fn, ret, args):
    """int|double|bool fn(const Temporal*, scalar). Generic shape, one scalar extra."""
    if len(args) != 2 or args[0] != "Temporal*" or args[1] not in _SCALAR_CPP:
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    inp = _infer_input(fn)
    if not rk or not inp:
        return None
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "scalar", "cpp": _SCALAR_CPP[args[1]]}], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: single-instant {inp} against a scalar -> {rk}.",
    }


def temporal_x_geom(fn, ret, args):
    """int|double|bool fn(const Temporal*, const GSERIALIZED*). Generic shape, one geom extra."""
    if args != ("Temporal*", "GSERIALIZED*"):
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    inp = _infer_input(fn)
    if not rk or not inp:
        return None
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "geom"}], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: single-instant {inp} against a static geometry -> {rk}.",
    }


def _result_extract_kind(fn):
    """Scalar return_kind for a Temporal*-returning transform whose single-instant
    result carries a tint/tfloat/tbool value — inferred from the function name."""
    if "_to_tint" in fn:
        return "extract_int"
    if "_to_tfloat" in fn:
        return "extract_double"
    if "_to_tbool" in fn:
        return "extract_bool"
    if fn.startswith("tfloat_"):
        return "extract_double"
    if fn.startswith("tint_"):
        return "extract_int"
    if fn.startswith("tbool_"):
        return "extract_bool"
    return None


def temporal_extract_scalar(fn, ret, args):
    """Unary Temporal->Temporal* transform whose single-instant result carries a
    scalar value (tfloat_ceil, tbool_to_tint, ...). Generic shape with an extract
    marshaler. Result/text/geo-returning transforms are deferred (varsize)."""
    if ret != "Temporal*" or args != ("Temporal*",):
        return None
    inp = _infer_input(fn)
    rk = _result_extract_kind(fn)
    if not inp or not rk:
        return None
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp, "extra_args": [], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: single-instant {inp} transform, value extracted -> {rk[8:]}.",
    }


SHAPES = {
    "cmp_scalar_tempfirst": cmp_scalar_tempfirst,
    "cmp_scalar_scalarfirst": cmp_scalar_scalarfirst,
    "cmp_two_temporal": cmp_two_temporal,
    "sprel_scalar_existing": sprel_scalar_existing,
    "temporal_unary_scalar": temporal_unary_scalar,
    "temporal_x_scalar": temporal_x_scalar,
    "temporal_x_geom": temporal_x_geom,
    "temporal_extract_scalar": temporal_extract_scalar,
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
