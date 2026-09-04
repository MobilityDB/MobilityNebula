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

The comparison classifiers (cmp_scalar_tempfirst, cmp_scalar_scalarfirst,
cmp_two_temporal) derive the primary Nebula name and SQL token from the
catalog's sqlfn field when --catalog is supplied.  For ever_eq_tfloat_float
with sqlfn="eEq" this produces nebula_name="EEqTfloatFloat" and
sql_token="E_EQ_TFLOAT_FLOAT" as the canonical primary, with
alias_sql_token="EVER_EQ_TFLOAT_FLOAT" preserved as the keep-both backward-
compat alias.  Without --catalog the classifiers fall back to the existing
pascal(fn) / fn.upper() naming with no alias emitted.

Usage:
  build_descriptor.py --sigs /tmp/cmp_sigs.txt --gap /tmp/nebula_gap.txt \\
                      --shapes cmp_scalar_tempfirst --out wave22.json \\
                      [--catalog /path/to/meos-idl.json]
"""
import argparse
import collections
import json
import re
import sys

# --- per-base-type input construction -------------------------------------
#
# Building a single-instant temporal value of type T needs its parser and the C
# type of its base value, and the catalog states both. MEOS names the parser
# `<T>_in` for every text input, and `<T>inst_make(value, TimestampTz)` takes
# the base value, so its FIRST parameter IS that C type — `tfloatinst_make`
# takes a double and `tintinst_make` an int. Reading them retires a table that
# named two temporal types where the catalog describes thirteen.
#
# The instant's text form `value@timestamp` is one grammar shared by every
# temporal type, so it is a constant rather than a per-type entry.
INSTANT_WKT = "{}@{}"
# C type of a by-value operand -> the C++ spelling a stream field carries. This
# is the binding's marshalling and not a MEOS fact, and it is ONE mapping: two
# copies of it disagreed on which widths exist, so a shape reading one admitted
# an operand the other declined. The two 64-bit widths are already what this
# emitter writes for a timestamp and for a network-point route id, so they name
# no new convention.
SCALAR_CPP = {"double": "double", "int": "int32_t", "bool": "bool",
              "int64_t": "int64_t", "uint64_t": "uint64_t"}

# temporal type -> its catalog facts (base, bbox, number/spatial/linear flags),
# populated by load_catalog().
_TEMPORAL_TYPES: dict = {}


def base_input(temporal_type):
    """(parser, base C type, wkt format) for a temporal type, or None.

    None means the catalog does not describe both halves, so a shape asking for
    this type declines rather than emitting a call to a parser or a constructor
    that may not exist.
    """
    parser = f"{temporal_type}_in"
    ctor = (_CATALOG.get(f"{temporal_type}inst_make") or {}).get("params") or []
    if parser not in _CATALOG or not ctor:
        return None
    return parser, ctor[0], INSTANT_WKT

# Catalog index populated by load_catalog(); maps fn-name -> the catalog facts a
# classifier may need: the canonical SQL name, and the header that declares the
# symbol. Indexing EVERY function, not only those carrying a sqlfn, is what lets
# a classifier ask the catalog where a symbol lives instead of keeping a
# hand-written map of its own.
_CATALOG: dict = {}


def load_catalog(path: str) -> None:
    """Index the composed meos-idl.json at *path* into _CATALOG.

    Every function is indexed. A missing sqlfn is recorded as empty rather than
    dropping the record, so a caller asking where a symbol is declared still
    gets an answer for a function that has no SQL name; the naming classifiers
    test the FIELD rather than the presence of the record. When --catalog is not
    supplied _CATALOG stays empty and the classifiers fall back to
    pascal(fn)/fn.upper() naming.
    """
    with open(path) as fh:
        data = json.load(fh)
    entries = data.get("functions", data) if isinstance(data, dict) else data
    for entry in entries:
        _CATALOG[entry["name"]] = {
            "sqlfn": entry.get("sqlfn") or "",
            "sqlop": entry.get("sqlop", ""),
            "file": entry.get("file") or "",
            # the C parameter types, so a caller can read a constructor's own
            # signature instead of restating what it takes
            "params": [_norm_ctype(p.get("cType")) for p in (entry.get("params") or [])],
            # what the function ANSWERS, so a caller needing a different pointer
            # type reads the conversion off the catalog instead of assuming one
            "returns": _norm_ctype((entry.get("returnType") or {}).get("c")),
            # the parameter names the function writes its answer through, which
            # the catalog states rather than leaving to be guessed from a `*`
            "outparams": ((entry.get("shape") or {}).get("outParams") or []),
            "paramnames": [p.get("name") for p in (entry.get("params") or [])],
        }
    _TEMPORAL_TYPES.update(data.get("temporalTypes") or {})
    _TYPE_ENCODINGS.update(data.get("typeEncodings") or {})
    _DERIVED_INPUTS.update(derived_inputs())


def catalog_header(fn):
    """The umbrella header DECLARING `fn`, as the catalog states it.

    A classifier needs to know which header the emitted operator must include to
    reach its MEOS symbol. Spelling that as a token-to-header map inside each
    classifier is the drift class this generator exists to remove: the map has
    to be edited for every family MEOS adds, and it is wrong the moment a symbol
    moves. Both maps this replaced sent every `trgeometry` symbol to
    `meos_pose.h` while all 132 of them are declared in `meos_rgeo.h`, which
    emits an include that does not declare what the operator calls.

    Returns [] for `meos.h`, which the emitted preamble already includes, and
    for anything that is not an umbrella header — a symbol reachable only
    through an internal header cannot be called by generated code at all.
    """
    header = (_CATALOG.get(fn) or {}).get("file") or ""
    if not header or header == "meos.h" or not _UMBRELLA.fullmatch(header):
        return []
    return [header]


def pascal(meos_call):
    """Convert snake_case MEOS name to PascalCase (used for type suffixes)."""
    return "".join(p.capitalize() for p in meos_call.split("_"))


def _pascal_sqlfn(s: str) -> str:
    """Capitalise only the first character of a camelCase sqlfn token.

    "eEq" -> "EEq", "aGe" -> "AGe", "tEq" -> "TEq".
    The internal camel casing is the SQL-name convention and must not be
    flattened by the full pascal() transform.
    """
    return s[0].upper() + s[1:] if s else s


def _camel_to_upper_snake(s: str) -> str:
    """Convert camelCase sqlfn to UPPER_SNAKE for a grammar token prefix.

    "eEq" -> "E_EQ", "aGe" -> "A_GE", "tNe" -> "T_NE".
    """
    return re.sub(r"([A-Z])", r"_\1", s).upper()


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
    Reuses build_tnumber_point_with_scalar (already in codegen_nebula.py).

    When _CATALOG carries a sqlfn for *fn* the descriptor uses the sqlfn-backed
    canonical name as primary and preserves fn.upper() as alias_sql_token.
    """
    parts = fn.split("_")
    if len(parts) < 2 or parts[0] not in ("ever", "always") or parts[1] not in ("eq", "ne", "lt", "le", "gt", "ge"):
        return None
    if ret != "int" or len(args) != 2 or args[0] != "Temporal*":
        return None
    if args[1] not in SCALAR_CPP:
        return None
    # as in the scalar-first twin, the NAME states the operand's temporal type
    base = _infer_input(fn)
    resolved = base_input(base) if base else None
    if not resolved:
        return None
    in_fn, vcpp, wkt = resolved
    # type suffix = everything after the two-word verb+comparator prefix
    type_suffix = "_".join(parts[2:])
    # a record now exists for every catalogued function, so the guard tests the
    # sqlfn FIELD; a function with no SQL name keeps the fallback naming
    cat = _CATALOG.get(fn) or {}
    if cat.get("sqlfn"):
        sqlfn = cat["sqlfn"]
        nebula_name = _pascal_sqlfn(sqlfn) + pascal(type_suffix)
        sql_token = _camel_to_upper_snake(sqlfn) + "_" + type_suffix.upper()
        alias_sql_token = fn.upper()
    else:
        nebula_name = pascal(fn)
        sql_token = fn.upper()
        alias_sql_token = None
    d = {
        "nebula_name": nebula_name,
        "sql_token": sql_token,
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
            f"Per-event {parts[0]} comparison of a single-instant {base} "
            f"(built from value+timestamp) against a scalar constant."),
    }
    if cat.get("sqlfn"):
        d["sqlfn"] = cat["sqlfn"]
        d["sqlop"] = cat["sqlop"]
    if alias_sql_token:
        d["alias_sql_token"] = alias_sql_token
    return d


def cmp_scalar_scalarfirst(fn, ret, args):
    """ever/always comparison: int fn(double|int, const Temporal*), scalar first.
    Reuses build_tnumber_scalar_first (MEOS call passes scalar as 1st arg).

    When _CATALOG carries a sqlfn for *fn* the descriptor uses the sqlfn-backed
    canonical name as primary and preserves fn.upper() as alias_sql_token.
    """
    parts = fn.split("_")
    if len(parts) < 2 or parts[0] not in ("ever", "always") or parts[1] not in ("eq", "ne", "lt", "le", "gt", "ge"):
        return None
    if ret != "int" or len(args) != 2 or args[1] != "Temporal*":
        return None
    if args[0] not in SCALAR_CPP:
        return None
    # Which temporal type the operand is, is stated by the NAME, not by the
    # scalar's C type: `always_eq_bigint_tbigint` names tbigint, and two
    # temporal types can share one C spelling anyway.
    base = _infer_input(fn)
    resolved = base_input(base) if base else None
    if not resolved:
        return None
    in_fn, vcpp, wkt = resolved
    type_suffix = "_".join(parts[2:])
    # a record now exists for every catalogued function, so the guard tests the
    # sqlfn FIELD; a function with no SQL name keeps the fallback naming
    cat = _CATALOG.get(fn) or {}
    if cat.get("sqlfn"):
        sqlfn = cat["sqlfn"]
        nebula_name = _pascal_sqlfn(sqlfn) + pascal(type_suffix)
        sql_token = _camel_to_upper_snake(sqlfn) + "_" + type_suffix.upper()
        alias_sql_token = fn.upper()
    else:
        nebula_name = pascal(fn)
        sql_token = fn.upper()
        alias_sql_token = None
    d = {
        "nebula_name": nebula_name,
        "sql_token": sql_token,
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
            f"Per-event {parts[0]} comparison of a scalar constant against a "
            f"single-instant {base} (built from value+timestamp); scalar-first MEOS arg order."),
    }
    if cat.get("sqlfn"):
        d["sqlfn"] = cat["sqlfn"]
        d["sqlop"] = cat["sqlop"]
    if alias_sql_token:
        d["alias_sql_token"] = alias_sql_token
    return d


def cmp_two_temporal(fn, ret, args):
    """Generic two-temporal comparison: int fn(const Temporal*, const Temporal*)
    on the *_temporal_temporal functions. Builds two single-instant tfloats from
    (valueA,tsA)/(valueB,tsB) and reuses build_two_tnumber_points.

    When _CATALOG carries a sqlfn for *fn* the descriptor uses the sqlfn-backed
    canonical name as primary and preserves fn.upper() as alias_sql_token.
    """
    if ret != "int" or args != ("Temporal*", "Temporal*") or not fn.endswith("_temporal_temporal"):
        return None
    resolved = base_input("tfloat")
    if not resolved:
        return None
    in_fn, vcpp, wkt = resolved
    # type suffix is always "temporal_temporal" for this shape
    type_suffix = "temporal_temporal"
    # a record now exists for every catalogued function, so the guard tests the
    # sqlfn FIELD; a function with no SQL name keeps the fallback naming
    cat = _CATALOG.get(fn) or {}
    if cat.get("sqlfn"):
        sqlfn = cat["sqlfn"]
        nebula_name = _pascal_sqlfn(sqlfn) + pascal(type_suffix)
        sql_token = _camel_to_upper_snake(sqlfn) + "_" + type_suffix.upper()
        alias_sql_token = fn.upper()
    else:
        nebula_name = pascal(fn)
        sql_token = fn.upper()
        alias_sql_token = None
    d = {
        "nebula_name": nebula_name,
        "sql_token": sql_token,
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
    if cat.get("sqlfn"):
        d["sqlfn"] = cat["sqlfn"]
        d["sqlop"] = cat["sqlop"]
    if alias_sql_token:
        d["alias_sql_token"] = alias_sql_token
    return d


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
# Which input machinery a function name asks for.
#
# Two kinds of token appear in a MEOS name. A CONCRETE temporal type names
# itself, and the catalog lists every one of them. A CLASS token — tnumber,
# tgeo, tpoint, tspatial — names a set rather than a type, and the catalog
# states the membership as per-type flags, so the class resolves to a member
# the emitter can actually build rather than to a name written down here.
#
# The buildable set is codegen_nebula's, not the catalog's: a type is only
# usable as an operand if that module carries a builder for it. Asking it
# directly keeps the two from drifting, which a table restating its keys cannot.
# ⛔ NOT DERIVABLE FROM THE CATALOG, and a measurement says so rather than a
# preference. A CLASS token — tnumber, tgeo, tpoint, tspatial — names a SET, and
# an operand has to be built as one concrete member of it. The catalog states
# the membership (the `number` and `spatial` flags of temporalTypes) but not
# WHICH member represents the class, because that is not a fact about MEOS: it
# is this emitter choosing what a `tspatial` operand shall be built as.
#
# The choice is not derivable, and the alternatives are not equivalent: picking
# the first buildable member alphabetically resolves `tspatial` to `tcbuffer`,
# so `above_stbox_tspatial` and 57 siblings parse a circular buffer where a
# geometry point is meant, and a generic shape then matches
# `acovers_trgeometry_geo` ahead of the dedicated trgeometry classifier. The
# values below ARE the choice; the catalog states the membership, not the
# representative.
_GENERIC_INPUT_FOR = [
    ("tgeompoint", "tgeompoint"), ("tgeogpoint", "tgeompoint"), ("tgeometry", "tgeometry"),
    ("tcbuffer", "tcbuffer"), ("tnpoint", "tnpoint"), ("tpose", "tpose"),
    ("tfloat", "tfloat"), ("tint", "tint"), ("tbool", "tbool"),
    ("tnumber", "tfloat"), ("tgeo", "tgeompoint"), ("tpoint", "tgeompoint"),
    ("tspatial", "tgeompoint"),
]



def scalar_base_input(temporal_type):
    """The event fields and MEOS constructor for a temporal type built from ONE
    by-value scalar, or None if the catalog does not describe one.

    `<T>inst_make(base, TimestampTz)` is the constructor MEOS names for every
    temporal type, and the catalog carries its parameter types, what it returns
    and the header declaring it. Where the base parameter is a plain by-value C
    type, the operand needs no text grammar at all: the event's single value
    goes straight into the constructor. That is what retires the per-type WKT
    table, whose eight hand-written entries were the generator's last statement
    of a fact the catalog already makes.

    Two cases decline rather than guess. A POINTER base -- a Cbuffer, a Pose, an
    Npoint, a geometry -- needs that value built first, which is a separate
    question. And a base whose C type this binding has no spelling for is
    declined because the width is not stated anywhere the generator can read:
    `S2CellId` accepts a uint64_t at the call site, so a wrong width would
    narrow in silence rather than fail to compile.
    """
    name = f"{temporal_type}inst_make"
    fn = _CATALOG.get(name)
    if not fn:
        return None
    params = fn.get("params") or []
    if len(params) != 2 or "*" in params[0]:
        return None
    cpp = SCALAR_CPP.get(params[0])
    if not cpp:
        return None
    spec = {
        "fields": [["value", cpp], ["ts", "uint64_t"]],
        "ctor": name,
        "headers": catalog_header(name),
    }
    # the constructors answer a TInstant *, which the operand slot holds as the
    # Temporal * it is the first member of; the catalog states which it is
    if fn.get("returns") != "Temporal*":
        spec["cast"] = "Temporal *"
    return spec


# temporal type -> its derived operand builder, filled by load_catalog(). A
# concrete type that HAS one needs no row in the class table below: the token
# names the type, and the type names its own constructor.
_DERIVED_INPUTS: dict = {}


def derived_inputs():
    """Every temporal type this generator can build an operand for, derived."""
    return {t: spec for t in sorted(_TEMPORAL_TYPES)
            if (spec := scalar_base_input(t))}



# C type name -> its catalog encoding entry, filled by load_catalog(). The
# encodings state the parser and serializer MEOS gives each type.
_TYPE_ENCODINGS: dict = {}


def text_value_input(ctype):
    """The operand builder for a static value carried as a text field, or None.

    A value of a static type -- a box, a circular buffer, a pose, a network point
    -- travels as the text its own `_out` writes and is read back with its `_in`.
    The catalog states both in `typeEncodings`, together with the header
    declaring the parser, so a shape needs to name neither.

    The parser is only unambiguous when it is named for the type itself: the
    encoding for `Span` is `bigintspan_in`, one variety of many, and a `Span *`
    argument does not say which. Such a type declines rather than parse an
    integer span where a timestamp span is meant.
    """
    bare = ctype.rstrip("*")
    enc = _TYPE_ENCODINGS.get(bare) or {}
    parser = enc.get("in")
    if not parser or parser != bare.lower() + "_in":
        # A type the encodings map does not describe may still declare the pair
        # itself. `text` is the case: the catalog states its marshalling in the
        # `wire` block as a plain JSON string rather than in typeEncodings, and
        # `text_in(const char *) -> text *` with `text_out(const text *) ->
        # char *` is the pair that builds one. Requiring the EXACT signatures is
        # what keeps this from admitting a variety-locked parser: `span_in` takes
        # a MeosType alongside the string, so a Span still declines here as it
        # does above.
        enc = _self_declared_text_codec(bare)
        if not enc:
            return None
        parser = enc["in"]
    aux = _parser_aux_args(parser, enc)
    if aux is None:
        return None
    return {
        "fields": [["box", "VariableSizedData"]],
        "parser": parser,
        "parser_aux": aux,
        "cpp_type": bare,
        "headers": catalog_header(parser),
    }


def _self_declared_text_codec(bare):
    """The text codec a type declares for itself, or None.

    Reads the catalog's own function list rather than its encodings map: a
    `<t>_in(const char *) -> T *` paired with a `<t>_out(const T *) -> char *`
    IS a text codec whatever the map says, and both have to be reachable for a
    generated operator to call them.
    """
    lo = bare.lower()
    dec, encf = _CATALOG.get(f"{lo}_in"), _CATALOG.get(f"{lo}_out")
    if not dec or not encf:
        return None
    if dec.get("params") != ["char*"] or dec.get("returns") != f"{bare}*":
        return None
    if (encf.get("params") or [None])[0] != f"{bare}*" or encf.get("returns") != "char*":
        return None
    return {"in": f"{lo}_in", "out": f"{lo}_out", "in_aux": [], "out_aux": [],
            "encoders": {"text": f"{lo}_out"}}


def _parser_aux_args(parser, enc):
    """The literal arguments a text parser takes AFTER the string, or None.

    Most parsers read a string and nothing else, but `interval_in` also takes a
    typmod, and the catalog says so twice over: the parser's own `params`, and
    the `in_aux` entry naming the argument and the value to pass when a caller
    has none. Counting the parameters rather than assuming one is what keeps a
    type whose parser takes a second argument from being called with one.
    """
    params = (_CATALOG.get(parser) or {}).get("params")
    if not params or params[0] != "char*":
        return None
    aux = enc.get("in_aux") or []
    if len(params) != 1 + len(aux):
        return None
    return "".join(f", {a.get('default', 0)}" for a in aux)


def wkb_value_codec(ctype):
    """The type-GENERIC hex-WKB codec pair for `ctype`, or None.

    The text form of a span states no variety: the catalog's own `in` for `Span`
    is `bigintspan_in`, one of many, so a `Span *` argument cannot be read from
    text without guessing which variety is meant, and `text_value_input`
    declines it for exactly that reason. The hex-WKB form carries the variety
    INSIDE the value, so ONE decoder answers every variety of the type, and MEOS
    names that decoder for the type itself -- `span_from_hexwkb`,
    `set_from_hexwkb`, `spanset_from_hexwkb`, `temporal_from_hexwkb`.

    Both halves are read off the catalog: the decoder from
    `typeEncodings[T].decoders.wkb`, the encoder as the `_as_hexwkb` sibling the
    catalog declares. A type MEOS gives no generic decoder, or no encoder to
    hand the value back on, declines rather than travel in a form it cannot
    round-trip.
    """
    bare = ctype.rstrip("*")
    enc = _TYPE_ENCODINGS.get(bare) or {}
    decoder = (enc.get("decoders") or {}).get("wkb")
    if decoder != bare.lower() + "_from_hexwkb":
        return None
    if (_CATALOG.get(decoder) or {}).get("params") != ["char*"]:
        return None
    encoder = bare.lower() + "_as_hexwkb"
    args = _hexwkb_encoder_args(encoder)
    if args is None:
        return None
    return {"parser": decoder, "serializer": encoder, "serializer_args": args,
            "cpp_type": bare, "headers": catalog_header(decoder)}


def _hexwkb_encoder_args(encoder):
    """The arguments a hex-WKB encoder takes AFTER the value, or None.

    The family is not one signature: `span_as_hexwkb` takes an endian variant
    and a length out-parameter, `raster_as_hexwkb` only the length, and
    `pcpoint_as_hexwkb` neither. Each parameter is read from the catalog and
    answered by what it IS -- the variant by the NDR default this binding
    already writes, the length by the local it fills -- so an encoder whose
    parameters say something else declines instead of being called wrongly.
    """
    params = (_CATALOG.get(encoder) or {}).get("params")
    if not params:
        return None
    out = []
    for param in params[1:]:
        if param == "uint8_t":
            out.append(", 0")
        elif param == "size_t*":
            out.append(", &hexSize")
        else:
            return None
    return "".join(out)


def operand_marshalling(ctype):
    """How one argument of `ctype` travels in a stream field, or None.

    ONE reading of an argument type, shared by every shape, so a type admitted
    by one is admitted by all. The order states the preference: a by-value
    scalar is a column of its own width; a geometry is the WKT its own field
    carries; a static value with a parser named for its type is that text; and
    anything else MEOS gives a generic hex-WKB codec travels in that form.

    Text before hex-WKB is what keeps the operators this generator already emits
    byte-identical: a Cbuffer and an STBox carry both encodings, and the text one
    is what their existing operators read.
    """
    if ctype in SCALAR_CPP:
        return {"kind": "scalar", "cpp": SCALAR_CPP[ctype]}
    if ctype == "GSERIALIZED*":
        return {"kind": "geom", "cpp_type": "GSERIALIZED"}
    # `rstrip("*")` reads `Cbuffer**` as `Cbuffer`, so without this a
    # pointer-to-pointer -- an array of values, or a value written back through
    # the argument -- would be marshalled as the single value it is not.
    if not ctype.endswith("*") or ctype.endswith("**"):
        return None
    spec = text_value_input(ctype)
    if spec:
        return {"kind": "box", "box_type": spec["cpp_type"],
                "cpp_type": spec["cpp_type"],
                "parser": spec["parser"], "parser_aux": spec["parser_aux"],
                "header": (spec["headers"] or ["meos.h"])[0]}
    spec = wkb_value_codec(ctype)
    if spec:
        return {"kind": "wkb_value", "value_type": spec["cpp_type"],
                "parser": spec["parser"],
                "header": (spec["headers"] or ["meos.h"])[0]}
    return None


def _text_encoder(bare):
    """(encoder, its arguments after the value) for the text form of `bare`.

    The encoder is `typeEncodings[T].encoders.text` and the arguments after the
    value are what `out_aux` names, counted against the encoder's own
    parameters. Reading the encoders entry rather than composing a name is what
    answers a geometry, whose text encoder is `geo_as_ewkt` and not the
    `<t>_out` every other type spells.
    """
    enc = _TYPE_ENCODINGS.get(bare) or {}
    encoder = (enc.get("encoders") or {}).get("text")
    if not encoder or encoder not in _CATALOG:
        return None
    header = (_CATALOG[encoder].get("file") or "")
    if not reachable_header(header):
        return None
    params = _CATALOG[encoder].get("params") or []
    aux = enc.get("out_aux") or []
    if len(params) != 1 + len(aux):
        return None
    args = "".join(f", {a.get('default', 0)}" for a in aux)
    return {"serializer": encoder, "serializer_args": args, "cpp_type": bare,
            "headers": catalog_header(encoder)}


# The temporal subtypes an operand builder already hands on as the Temporal it
# begins with. `scalar_base_input` casts a constructor's TInstant * for exactly
# this reason; a function ANSWERING one states the same fact from the other side.
_TEMPORAL_SUBTYPES = ("TInstant*", "TSequence*", "TSequenceSet*")


def result_marshalling(ret):
    """How the result of a function comes back out, as a return_kind, or None.

    A number is the value itself. Anything else leaves in THE SAME ENCODING the
    operand side reads for that type, so the output of one operator is the input
    of the next: a geometry as the EWKT a geometry operand is parsed from, a
    circular buffer as the text its own `_in` reads back, a span as the hex-WKB
    that is the only form stating which variety it is.

    Asking `operand_marshalling` for the encoding, rather than picking one here,
    is what keeps the two sides from disagreeing -- an answer serialized in a
    form the operand side cannot parse composes with nothing.
    """
    if ret in ("bool", "int", "double"):
        return ret, None
    if ret in _TEMPORAL_SUBTYPES:
        # A TInstant, a TSequence and a TSequenceSet each BEGIN with the Temporal
        # they are a kind of, so the Temporal encoder serializes one and the
        # exchange form is the same hex-WKB every other temporal travels in. The
        # operand side already reads this off the catalog when a constructor
        # answers a TInstant *; answering one is the same fact.
        spec = wkb_value_codec("Temporal*")
        if spec:
            # the local keeps the type the function ANSWERS; the cast is applied
            # where the Temporal encoder is called, so neither side is a lie
            return "wkb", dict(spec, cpp_type=ret.rstrip("*"), cast="Temporal *")
        return None, None
    marsh = operand_marshalling(ret)
    if not marsh:
        return None, None
    if marsh["kind"] == "wkb_value":
        spec = wkb_value_codec(ret)
        return ("wkb", spec) if spec else (None, None)
    if marsh["kind"] in ("box", "geom"):
        spec = _text_encoder(marsh["cpp_type"])
        return ("wkb", spec) if spec else (None, None)
    return None, None


def primary_operand_spec(marsh, index):
    """The `input_spec` for the operand the emitted lambda builds first.

    The emitter builds ONE operand ahead of the call and passes the rest as
    extra arguments; which of the function's arguments that is depends only on
    which one needs building, so it is read here rather than assumed to be the
    first.
    """
    if marsh["kind"] == "box":
        return {"fields": [["box", "VariableSizedData"]],
                "parser": marsh["parser"], "parser_aux": marsh["parser_aux"],
                "cpp_type": marsh["box_type"], "headers": [marsh["header"]]}
    if marsh["kind"] == "geom":
        # The geometry is read with `geom_in`, which the binding already uses
        # for a geometry ARGUMENT and which accepts the SRID-carrying EWKT the
        # result side writes. Parsing it directly rather than through
        # StaticGeometry leaves the value owned by the lambda, which is what the
        # single `free(temp)` at the end of the call expects.
        return {
            "fields": [["geom", "VariableSizedData"]],
            "header": "meos_geo.h", "headers": ["meos_geo.h"],
            "cpp_type": "GSERIALIZED",
            "build": ('                std::string {var}S(geomPtr, geomSize);\n'
                      '                while (!{var}S.empty() && ({var}S.front()==\'\\\'\' || {var}S.front()==\'"\')) {var}S.erase({var}S.begin());\n'
                      '                while (!{var}S.empty() && ({var}S.back()==\'\\\'\' || {var}S.back()==\'"\')) {var}S.pop_back();\n'
                      '                GSERIALIZED* {var} = geom_in({var}S.c_str(), -1);\n'
                      '                if (!{var}) return {z};\n'),
        }
    if marsh["kind"] == "wkb_value":
        t, parser = marsh["value_type"], marsh["parser"]
        return {
            "fields": [["val", "VariableSizedData"]],
            "header": marsh["header"],
            "headers": [marsh["header"]],
            "cpp_type": t,
            "build": ('                std::string {var}Hex(valPtr, valSize);\n'
                      f'                {t}* {{var}} = {parser}({{var}}Hex.c_str());\n'
                      '                if (!{var}) return {z};\n'),
        }
    return None


# The scalar answers a stream field can carry back, and the zero each falls to
# when the function reports it found none.
_OUT_RETURNS = {"double": "double", "int": "int", "bool": "bool"}


def trailing_out_param(fn, ret, args):
    """(index, return kind) for a function whose answer is a trailing out-param.

    `stbox_tmax(const STBox *, TimestampTz *result)` answers `bool` and writes
    the value through `result`; the catalog says so in `shape.outParams` rather
    than leaving a `*` to be read as an out-parameter by shape. A per-event
    operator expresses that directly -- declare the local, pass its address,
    return it -- so the found-flag becomes the operator's own empty answer.

    Only a LAST parameter qualifies, and only for a scalar this binding can hand
    back: an out-param in the middle would reorder the call, and a type with no
    return kind has nowhere to put the answer.
    """
    facts = _CATALOG.get(fn) or {}
    outs = facts.get("outparams") or []
    names = facts.get("paramnames") or []
    if ret != "bool" or len(outs) != 1 or len(names) != len(args):
        return None
    try:
        idx = names.index(outs[0])
    except ValueError:
        return None
    if idx != len(args) - 1 or not args[idx].endswith("*"):
        return None
    kind = _OUT_RETURNS.get(args[idx][:-1])
    return (idx, kind) if kind else None


def value_marshalled(fn, ret, args):
    """Any function whose result and EVERY argument the catalog can marshal.

    The shapes above each name one arity and one operand vocabulary, so a
    function differing from a covered one only in which type its second argument
    takes falls out of the surface entirely. This one asks the catalog the same
    question of every argument -- can a stream field carry a value of this type,
    and can the answer be handed back -- and emits whenever all of them say yes.
    It sits LAST, so every named shape keeps its own naming, ordering and
    comments, and this answers only what none of them claimed.

    The operand that gets BUILT is the first argument that needs building; the
    rest travel as extra arguments in their own order, and `primary_index`
    records where the built one goes back in the call. A function of scalars
    alone declines: it has no operand to build, so it is not a per-event
    operator over a MEOS value.
    """
    if not args:
        return None
    marsh = [operand_marshalling(a) for a in args]
    if not all(marsh):
        return None
    return_kind, result = result_marshalling(ret)
    if not return_kind:
        return None
    primary = next((i for i, m in enumerate(marsh) if m["kind"] != "scalar"), None)
    if primary is None:
        return None
    spec = primary_operand_spec(marsh[primary], primary)
    if not spec:
        return None
    headers = set(catalog_header(fn)) | set(spec.get("headers") or [])
    extras = []
    for i, m in enumerate(marsh):
        if i == primary:
            continue
        extras.append(m)
        if m["kind"] == "geom":
            headers.add("meos_geo.h")
        elif m["kind"] in ("box", "wkb_value"):
            headers.add(m["header"])
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "return_kind": return_kind,
        "input_type": spec["cpp_type"].lower() + "_value",
        "input_spec": spec, "extra_args": extras, "primary_index": primary,
        "comment_one_liner":
            f"Per-event {fn}: {len(args)} catalog-marshalled operand(s) -> {return_kind}.",
    }
    if result:
        d["result_type"] = result["cpp_type"]
        if result.get("cast"):
            d["result_cast"] = result["cast"]
        d["result_serializer"] = result["serializer"]
        d["result_serializer_args"] = result["serializer_args"]
        headers.update(result["headers"])
    headers.discard("meos.h")
    if headers:
        d["extra_headers"] = sorted(headers)
    return d


def _operand_phrase(inp):
    """How to name an operand of this input type in a generated comment.

    Every per-type builder assembles ONE instant from the event's fields, but
    the hex-WKB builder parses whatever temporal the field carries, which is a
    whole value and not an instant.
    """
    return ("a hex-WKB temporal" if inp == "wkb_temporal"
            else f"single-instant {inp}")


def _infer_input(fn):
    """The input machinery the operand of `fn` needs.

    A MEOS name states its SUBJECT first, so the operand is the type token
    appearing EARLIEST in the name. Taking the first entry of the table instead
    reads `tcbuffer_to_tgeompoint` as a tgeompoint operand, which is its RESULT:
    a conversion names what it produces after what it consumes, and 13 of the
    19 conversions invert under table order.
    """
    # When the leading token names a temporal type, that type IS the operand,
    # and a later token in the name is the RESULT — `tbigint_to_tfloat` consumes
    # a tbigint. If nothing can build that type the shape declines rather than
    # falling through to the result, which would marshal the wrong operand.
    # a concrete type with a derived builder is its own operand; the class
    # table states only which member a CLASS token stands for
    candidates = [(t, t) for t in _DERIVED_INPUTS] + _GENERIC_INPUT_FOR
    head = fn.split("_", 1)[0]
    if head in _TEMPORAL_TYPES:
        return dict(candidates).get(head)
    best = None
    for tok, inp in candidates:
        at = None
        if fn.startswith(tok + "_"):
            at = 0
        elif ("_" + tok + "_") in fn:
            at = fn.find("_" + tok + "_") + 1
        elif fn.endswith("_" + tok):
            at = len(fn) - len(tok)
        if at is not None and (best is None or at < best[0]):
            best = (at, inp)
    if best:
        return best[1]
    # A name whose operand token is the bare `temporal` states no concrete type,
    # so no per-type builder applies and the operand arrives already serialized:
    # the hex-WKB builder parses whichever temporal it carries.
    if "temporal" in fn.split("_"):
        return "wkb_temporal"
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
        "comment_one_liner": f"Per-event {fn}: {rk} accessor over {_operand_phrase(inp)}.",
    }





def temporal_x_scalar(fn, ret, args):
    """int|double|bool fn(const Temporal*, scalar). Generic shape, one scalar extra."""
    if len(args) != 2 or args[0] != "Temporal*" or args[1] not in SCALAR_CPP:
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    inp = _infer_input(fn)
    if not rk or not inp:
        return None
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "scalar", "cpp": SCALAR_CPP[args[1]]}], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: {_operand_phrase(inp)} against a scalar -> {rk}.",
    }


def temporal_x_geom(fn, ret, args):
    """int|double|bool fn(const Temporal*, const GSERIALIZED*). Generic shape, one geom extra."""
    geom_first = args == ("GSERIALIZED*", "Temporal*")
    if not geom_first and args != ("Temporal*", "GSERIALIZED*"):
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    inp = _infer_input(fn)
    if not rk or not inp:
        return None
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "geom"}], "return_kind": rk,
        "comment_one_liner": f"Per-event {fn}: {_operand_phrase(inp)} against a static geometry -> {rk}.",
    }
    if geom_first:
        d["literal_first"] = True
    return d


def temporal_unary_transform(fn, ret, args):
    """Temporal* fn(const Temporal*) — a per-event transform of one operand.

    The operand arrives as its event fields and the result leaves as hex-WKB in
    the arena, the exchange form two_temporal_temporal already uses, so one
    operator's output can feed the next.
    """
    if args != ("Temporal*",) or ret != "Temporal*":
        return None
    inp = _infer_input(fn)
    if not inp:
        return None
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp, "extra_args": [],
        "return_kind": "wkb",
        "comment_one_liner": f"Per-event {fn}: {_operand_phrase(inp)} -> hex-WKB temporal.",
    }
    hdr = catalog_header(fn)
    if hdr:
        d["extra_headers"] = hdr
    return d


def temporal_geom_transform(fn, ret, args):
    """Temporal* fn(const Temporal*, const GSERIALIZED*) — a transform against a
    static geometry literal, the result serialized to hex-WKB."""
    geom_first = args == ("GSERIALIZED*", "Temporal*")
    if ret != "Temporal*" or (not geom_first and args != ("Temporal*", "GSERIALIZED*")):
        return None
    inp = _infer_input(fn)
    if not inp:
        return None
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "geom"}], "return_kind": "wkb",
        "comment_one_liner": (
            f"Per-event {fn}: {_operand_phrase(inp)} against a static geometry "
            "-> hex-WKB temporal."),
    }
    if geom_first:
        d["literal_first"] = True
    hdr = catalog_header(fn)
    if hdr:
        d["extra_headers"] = hdr
    return d


def temporal_scalar_transform(fn, ret, args):
    """Temporal* fn(const Temporal*, scalar) — a transform against a scalar
    literal, the result serialized to hex-WKB."""
    if len(args) != 2 or args[0] != "Temporal*" or args[1] not in SCALAR_CPP:
        return None
    if ret != "Temporal*":
        return None
    inp = _infer_input(fn)
    if not inp:
        return None
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp,
        "extra_args": [{"kind": "scalar", "cpp": SCALAR_CPP[args[1]]}],
        "return_kind": "wkb",
        "comment_one_liner": (
            f"Per-event {fn}: {_operand_phrase(inp)} against a {args[1]} literal "
            "-> hex-WKB temporal."),
    }
    hdr = catalog_header(fn)
    if hdr:
        d["extra_headers"] = hdr
    return d


def _result_temporal_type(fn):
    """The temporal type a transform's result carries.

    A conversion names its result after `_to_`, and any other transform answers
    in its subject's own type: `tfloat_ceil` answers a tfloat. Both readings are
    checked against the catalog's temporal types, so a name that merely looks
    like one resolves to nothing.
    """
    cand = fn.split("_to_")[-1] if "_to_" in fn else fn.split("_", 1)[0]
    return cand if cand in _TEMPORAL_TYPES else None


# C type of the extracted value -> the marshaller that carries it. The names on
# the right belong to this emitter, so they are stated here; which MEOS type
# each corresponds to is read from the catalog.
_EXTRACT_KIND = {"int": "extract_int", "double": "extract_double", "bool": "extract_bool"}


def _result_extract(fn):
    """(return_kind, accessor) for a transform whose instant carries a scalar.

    The result's temporal type decides both: its base C type selects the
    marshaller, and MEOS reads the value of such an instant through
    `<type>_start_value`. None where the catalog describes neither, so the shape
    declines rather than naming an accessor that may not exist.
    """
    result = _result_temporal_type(fn)
    resolved = base_input(result) if result else None
    if not resolved:
        return None
    kind = _EXTRACT_KIND.get(resolved[1])
    accessor = f"{result}_start_value"
    if not kind or accessor not in _CATALOG:
        return None
    return kind, accessor


def temporal_extract_scalar(fn, ret, args):
    """Unary Temporal->Temporal* transform whose single-instant result carries a
    scalar value (tfloat_ceil, tbool_to_tint, ...). Generic shape with an extract
    marshaler. Result/text/geo-returning transforms are deferred (varsize)."""
    if ret != "Temporal*" or args != ("Temporal*",):
        return None
    inp = _infer_input(fn)
    extract = _result_extract(fn)
    if not inp or not extract:
        return None
    rk, accessor = extract
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp, "extra_args": [], "return_kind": rk,
        # the descriptor names the accessor, so the emitter needs no table of
        # which MEOS function reads the value of which temporal type
        "extract_fn": accessor,
        "comment_one_liner": f"Per-event {fn}: {_operand_phrase(inp)} transform, value extracted -> {rk[8:]}.",
    }


# A box literal operand is resolved from the catalog, not from a table.
#
# Parsing one at runtime needs three facts: the C++ type to declare, the MEOS
# function that parses its text form, and the header declaring that function.
# All three are catalogued. The C++ type is the argument's own C type without
# its pointer; the parser is `<token>_in`, the naming rule MEOS follows for
# every text input; and the header is where the catalog says that parser is
# declared. A table spelling the same three facts has to be extended for every
# box type MEOS adds, and says nothing when one is missing.


def _box_operand(token, ctype):
    """(cpp type, parser, header) for a box literal, or None when uncatalogued.

    `token` is the type name the function name states, `ctype` its C argument
    type. Returning None when the catalog carries no `<token>_in` is what keeps
    this honest: the shape declines rather than emitting a call to a parser that
    does not exist.
    """
    parser = f"{token}_in"
    if parser not in _CATALOG:
        return None
    header = catalog_header(parser)
    return ctype.replace("*", "").strip(), parser, (header[0] if header else "meos.h")


def _box_token_from_name(fn):
    """The type token the function name ends with, when the catalog parses it."""
    tail = fn.rsplit("_", 1)[-1]
    return tail if f"{tail}_in" in _CATALOG else None


def temporal_x_box(fn, ret, args):
    """int|double|bool fn over a Temporal and an STBox/TBox/Span query LITERAL,
    in EITHER argument order (e.g. `left_tspatial_stbox(temp, box)` and
    `above_stbox_tspatial(box, temp)`). The literal is parsed at runtime
    (stbox_in/tbox_in/tstzspan_in). For the temporal-first form the box type is
    the name suffix (distinguishing tstzspan from numspan); for the box-first
    form it is the C arg type (STBox/TBox)."""
    if len(args) != 2:
        return None
    rk = {"int": "int", "double": "double", "bool": "bool"}.get(ret)
    inp = _infer_input(fn)
    if not rk or not inp:
        return None
    literal_first = False
    if args[0] == "Temporal*" and args[1] in ("STBox*", "TBox*", "Span*"):
        tok = _box_token_from_name(fn)
        operand = _box_operand(tok, args[1]) if tok else None
        if not operand:
            return None
        bt, parser, hdr = operand
    elif args[1] == "Temporal*" and args[0] in ("STBox*", "TBox*"):
        # Box-first: the name ends with the TEMPORAL operand, so the box type
        # comes from its C argument type. A bare `Span*` is excluded here on
        # purpose — it names no single type (tstzspan, floatspan, numspan all
        # wear it), so only the temporal-first form, where the name states the
        # token, can resolve one.
        tok = args[0].replace("*", "").strip().lower()
        operand = _box_operand(tok, args[0])
        if not operand:
            return None
        bt, parser, hdr = operand
        literal_first = True
    else:
        return None
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": inp, "return_kind": rk,
        "extra_args": [{"kind": "box", "box_type": bt, "parser": parser, "header": hdr}],
        "comment_one_liner": f"Per-event {fn}: {_operand_phrase(inp)} against a {bt} literal -> {rk}.",
    }
    if literal_first:
        d["literal_first"] = True
    return d


def two_static_values(fn, ret, args):
    """bool|int|double fn(const T*, const T*) over two STATIC values of one type,
    each carried as the text its own `_out` writes -- a box, a circular buffer, a
    pose, a network point. The first is the primary operand and the second a
    `box`-kind extra arg; both are parsed by the type's `_in` and freed.

    This is the cross-vehicle comparison of two per-vehicle aggregates (GROUP BY
    vehicle_id, compared pairwise downstream) for a box, and the plain comparison
    of two literals for the rest.

    Which type this is, and how it is parsed, comes from the catalog rather than
    from a name written here, so every type MEOS gives a matching `_in` is
    answered by this one shape."""
    if len(args) != 2 or args[0] != args[1] or not args[0].endswith("*"):
        return None
    rk = {"bool": "bool", "double": "double", "int": "int"}.get(ret)
    if not rk:
        return None
    spec = text_value_input(args[0])
    if not spec:
        return None
    bt = spec["cpp_type"]
    hdr = (spec["headers"] or ["meos.h"])[0]
    return {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": bt.lower() + "_text", "return_kind": rk,
        "input_spec": spec,
        "extra_args": [{"kind": "box", "box_type": bt,
                        "parser": spec["parser"], "header": hdr}],
        "comment_one_liner": f"Per-event {fn}: two per-vehicle extent {bt}es -> {rk}.",
    }


def two_temporal_scalar(fn, ret, args):
    """bool|int|double fn(const Temporal*, const Temporal*) over TWO temporal
    operands carried as hex-WKB VARSIZED fields — the cross-vehicle f(trajA, trajB)
    scalar shape (e.g. two per-vehicle aggregate outputs compared in a self-join).
    The first temporal is the primary wkb_temporal input; the second is a
    wkb_temporal extra arg; both temporal_from_hexwkb-parsed and freed."""
    if len(args) != 2 or args[0] != "Temporal*" or args[1] != "Temporal*":
        return None
    rk = {"bool": "bool", "int": "int", "double": "double"}.get(ret)
    if not rk:
        return None
    # the meos_call's type may live outside meos.h/meos_geo.h; the catalog says where
    extra_headers = catalog_header(fn)
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": "wkb_temporal", "return_kind": rk,
        "extra_args": [{"kind": "wkb_temporal"}],
        "comment_one_liner": f"Per-event {fn}: two hex-WKB temporal operands -> {rk}.",
    }
    if extra_headers:
        d["extra_headers"] = extra_headers
    return d


def two_temporal_temporal(fn, ret, args):
    """Temporal* fn(const Temporal*, const Temporal*) over TWO temporal operands
    carried as hex-WKB VARSIZED fields, returning a Temporal* serialized back to
    hex-WKB VARSIZED — the cross-vehicle f(trajA, trajB) -> trajectory shape
    (temporal distance / arithmetic / topology between two per-vehicle aggregate
    outputs in a self-join). Both operands temporal_from_hexwkb-parsed and freed;
    the result temporal_as_hexwkb-serialized into the arena and the MEOS result
    freed. The output VARSIZED can itself feed another per-event MEOS operator."""
    if len(args) != 2 or args[0] != "Temporal*" or args[1] != "Temporal*":
        return None
    if ret != "Temporal*":
        return None
    # the meos_call symbol may live outside meos.h/meos_geo.h; the catalog says where
    extra_headers = catalog_header(fn)
    d = {
        "nebula_name": pascal(fn), "sql_token": fn.upper(), "meos_call": fn,
        "build_generic": True, "input_type": "wkb_temporal", "return_kind": "wkb",
        "extra_args": [{"kind": "wkb_temporal"}],
        "comment_one_liner": f"Per-event {fn}: two hex-WKB temporal operands -> hex-WKB temporal.",
    }
    if extra_headers:
        d["extra_headers"] = extra_headers
    return d


# --- Trgeometry spatial predicate shapes (W148/W149 wave) -------------------
# The current MEOS-API run.py correctly resolves GSERIALIZED* in the IDL, so
# parse_sigs normalizes `const GSERIALIZED *` -> "GSERIALIZED*" (strip-const +
# first-token + "*"). Classifiers use GSERIALIZED* for every geometry argument.
#
# Five classifiers, each selecting the correct emit_trgeometry_operator build key:
#
#   trgeometry_geo_predicate      (Temporal*, GSERIALIZED*)  int  — e/a intersects/disjoint/covers/touches
#   geo_trgeometry_predicate      (GSERIALIZED*, Temporal*)  int  — geo-first: econtains/acontains/ecovers/acovers
#   trgeometry_geo_dwithin        (Temporal*, GSERIALIZED*, double) int — edwithin/adwithin with distance
#   trgeometry_trgeometry_predicate (Temporal*, Temporal*) int — both-trgeometry predicates
#   trgeometry_trgeometry_dwithin  (Temporal*, Temporal*, double) int — both-trgeometry dwithin
#
# Each covers the "compact" (trgeometryinst_make) subtree; the "nad" subtypes
# (ever_eq/always_eq/nad_) are caught by the same arg-shape but different name
# prefixes. The classifier checks name tokens to pick the right build key.

_TRGEO_NAME_IS_EQ_NE = frozenset(["ever_eq", "ever_ne", "always_eq", "always_ne"])


def _is_eq_ne(fn):
    return any(fn.startswith(p + "_") for p in _TRGEO_NAME_IS_EQ_NE)


def _trgeo_brief(fn):
    """Derive the short brief string matching the probe's comment_one_liner."""
    verb = fn.split("_")[0]          # eintersects / adisjoint / ever / always / nad
    if verb == "nad":
        if fn.endswith("_geo"):
            return ("Returns the nearest approach distance between a 2D trgeometry "
                    "instant and a static geometry.")
        return "Returns the nearest approach distance between two 2D trgeometry instants."
    if verb in ("ever", "always"):
        # ever_eq / ever_ne / always_eq / always_ne
        op2 = fn.split("_")[1]   # eq or ne
        adj = "equal" if op2 == "eq" else "not equal"
        quant = "always" if verb == "always" else "ever"
        if "_trgeometry_geo" in fn:
            return (f"Returns 1.0 if the 2D trgeometry instant is {quant} {adj} "
                    f"to the static geometry.")
        if "_geo_trgeometry" in fn:
            return (f"Returns 1.0 if the static geometry is {quant} {adj} "
                    f"to the 2D trgeometry instant.")
        return f"Returns 1.0 if the two 2D trgeometry instants are {quant} {adj}."
    # dwithin variants have a dist arg — use a specific phrasing
    if verb in ("edwithin", "adwithin"):
        if "_trgeometry_geo" in fn:
            return (f"Returns 1 if the trgeometry instant is within dist of the geometry "
                    f"({verb}).")
        return f"Returns 1 if the two trgeometry instants are within dist ({verb})."
    # remaining spatial predicates: eintersects / adisjoint / ecovers / …
    if "_geo_trgeometry" in fn:
        return f"Returns 1 if the geometry {verb} the trgeometry instant."
    if "_trgeometry_geo" in fn:
        return f"Returns 1 if the trgeometry instant {verb} the geometry."
    return f"Returns 1 if the two trgeometry instants {verb}."


def trgeometry_geo_predicate(fn, ret, args):
    """int fn(Temporal*, GSERIALIZED*) — trgeometry_geo spatial predicates and eq/ne.

    Routes to build_trgeometry_geo (compact, trgeometryinst_make) for the
    spatial-predicate families (eintersects/adisjoint/ecovers/etouches/…) and
    to build_trgeometry_geo_nad (nad, trgeoinst_make) for ever_eq/always_eq/…"""
    if ret != "int" or args != ("Temporal*", "GSERIALIZED*"):
        return None
    # Must be a trgeometry function (not a plain tgeo/tpoint that hits "GSERIALIZED*" too)
    if "trgeometry" not in fn:
        return None
    build_key = "build_trgeometry_geo_nad" if _is_eq_ne(fn) else "build_trgeometry_geo"
    d = {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "int",
        "nautilus_return": "INT32",
        build_key: True,
        "comment_one_liner": _trgeo_brief(fn),
    }
    return d


def geo_trgeometry_predicate(fn, ret, args):
    """int fn(GSERIALIZED*, Temporal*) — geo-first trgeometry predicates and eq/ne.

    Routes to build_geo_trgeometry (compact, trgeometryinst_make) for spatial
    predicates (econtains/acontains/ecovers/acovers_geo_trgeometry) and to
    build_geo_trgeometry_eq (nad, trgeoinst_make) for ever_eq/always_eq geo-first."""
    if ret != "int" or args != ("GSERIALIZED*", "Temporal*"):
        return None
    if "trgeometry" not in fn:
        return None
    build_key = "build_geo_trgeometry_eq" if _is_eq_ne(fn) else "build_geo_trgeometry"
    d = {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "int",
        "nautilus_return": "INT32",
        build_key: True,
        "comment_one_liner": _trgeo_brief(fn),
    }
    return d


def trgeometry_geo_dwithin(fn, ret, args):
    """int fn(Temporal*, GSERIALIZED*, double) — trgeometry_geo dwithin with distance arg."""
    if ret != "int" or args != ("Temporal*", "GSERIALIZED*", "double"):
        return None
    if "trgeometry" not in fn:
        return None
    d = {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "int",
        "nautilus_return": "INT32",
        "build_trgeometry_geo_with_dist": True,
        "comment_one_liner": _trgeo_brief(fn),
    }
    return d


def trgeometry_trgeometry_predicate(fn, ret, args):
    """int fn(Temporal*, Temporal*) — trgeometry-vs-trgeometry predicates.

    Routes to build_trgeometry_trgeometry (compact) for spatial predicates and
    to build_trgeometry_trgeometry_nad (nad, trgeoinst_make) for ever_eq/always_eq.
    Also covers nad_trgeometry_trgeometry (double) via the double branch below —
    this classifier handles only int-returning variants."""
    if ret != "int" or args != ("Temporal*", "Temporal*"):
        return None
    if "trgeometry" not in fn:
        return None
    build_key = ("build_trgeometry_trgeometry_nad" if _is_eq_ne(fn)
                 else "build_trgeometry_trgeometry")
    d = {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "int",
        "nautilus_return": "INT32",
        build_key: True,
        "comment_one_liner": _trgeo_brief(fn),
    }
    return d


def trgeometry_trgeometry_dwithin(fn, ret, args):
    """int fn(Temporal*, Temporal*, double) — trgeometry dwithin with distance."""
    if ret != "int" or args != ("Temporal*", "Temporal*", "double"):
        return None
    if "trgeometry" not in fn:
        return None
    d = {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "int",
        "nautilus_return": "INT32",
        "build_trgeometry_trgeometry_with_dist": True,
        "comment_one_liner": _trgeo_brief(fn),
    }
    return d


def trgeometry_nad(fn, ret, args):
    """double fn(Temporal*, GSERIALIZED* | Temporal*) — nad nearest-approach-distance.

    Two sub-shapes: trgeometry_geo (Temporal*, GSERIALIZED*) and trgeometry_trgeometry
    (Temporal*, Temporal*). Both emit double-returning nad layouts. The two-temporal
    branch requires fn.endswith("_trgeometry") to exclude nad_trgeometry_tpoint."""
    if ret != "double" or "trgeometry" not in fn or not fn.startswith("nad_"):
        return None
    if args == ("Temporal*", "GSERIALIZED*"):
        build_key = "build_nad_trgeometry_geo"
    elif args == ("Temporal*", "Temporal*") and fn.endswith("_trgeometry"):
        build_key = "build_nad_trgeometry_trgeometry"
    else:
        return None
    return {
        "nebula_name": "".join(p.capitalize() for p in fn.split("_")),
        "sql_token": fn.upper(),
        "meos_call": fn,
        "return_type": "double",
        "nautilus_return": "FLOAT64",
        build_key: True,
        "comment_one_liner": _trgeo_brief(fn),
    }


SHAPES = {
    "cmp_scalar_tempfirst": cmp_scalar_tempfirst,
    "cmp_scalar_scalarfirst": cmp_scalar_scalarfirst,
    "cmp_two_temporal": cmp_two_temporal,
    "two_temporal_scalar": two_temporal_scalar,
    "two_temporal_temporal": two_temporal_temporal,
    "sprel_scalar_existing": sprel_scalar_existing,
    "temporal_unary_scalar": temporal_unary_scalar,
    "temporal_x_scalar": temporal_x_scalar,
    "temporal_x_geom": temporal_x_geom,
    "temporal_extract_scalar": temporal_extract_scalar,
    "temporal_x_box": temporal_x_box,
    "temporal_unary_transform": temporal_unary_transform,
    "temporal_geom_transform": temporal_geom_transform,
    "temporal_scalar_transform": temporal_scalar_transform,
    "two_static_values": two_static_values,
    "trgeometry_geo_predicate": trgeometry_geo_predicate,
    "geo_trgeometry_predicate": geo_trgeometry_predicate,
    "trgeometry_geo_dwithin": trgeometry_geo_dwithin,
    "trgeometry_trgeometry_predicate": trgeometry_trgeometry_predicate,
    "trgeometry_trgeometry_dwithin": trgeometry_trgeometry_dwithin,
    "trgeometry_nad": trgeometry_nad,
    "value_marshalled": value_marshalled,
}


# --- the catalog IS the input ----------------------------------------------
#
# A header dump plus a hand-written gap list is two hand-maintained inputs
# standing between the generator and the single source of truth, and it is the
# anti-pattern the North Star names: a binding is a pure catalog projection,
# never a hand list. The catalog already carries every signature and every
# function's api/category/file, so both inputs are derivable and neither has to
# be curated. `--sigs`/`--gap` stay accepted so an existing invocation keeps
# working; `--catalog` alone is sufficient and is the canonical form.

# The umbrella headers a consumer may include: meos.h and meos_<family>.h, and
# never an internal one. A symbol declared only in an internal header cannot be
# called by generated code even when the catalog calls it public.
_UMBRELLA = re.compile(r"meos(_[a-z0-9]+)?\.h$")

# Categories that carry a per-event streamable operator. io/constructor/
# lifecycle/index/aggregate describe a value's life or its text form, not a
# transformation of a stream element, and are residue by category rather than
# by shape.
_STREAMABLE_CATEGORIES = {"predicate", "transformation", "accessor", "setop"}


def _norm_ctype(c):
    """Normalize a catalog C type to what parse_sigs produces for a header."""
    c = re.sub(r"^\s*const\s+", "", (c or "").strip())
    stars = c.count("*")
    base = c.replace("*", "").strip().split()
    if not base:
        return ""
    return base[0] + ("*" * stars if stars else "")


def catalog_sigs(path):
    """name -> (ret, (arg-type, ...)), the parse_sigs mapping read from the catalog."""
    out = {}
    for f in json.load(open(path))["functions"]:
        ret = _norm_ctype((f.get("returnType") or {}).get("c"))
        args = tuple(_norm_ctype(p.get("cType")) for p in (f.get("params") or []))
        out[f["name"]] = (ret, tuple(a for a in args if a and a != "void"))
    return out


def catalog_candidates(path):
    """Every public function a consumer can actually call, with its catalog facts."""
    out = {}
    for f in json.load(open(path))["functions"]:
        if f.get("api") != "public":
            continue
        out[f["name"]] = {"file": f.get("file") or "",
                          "category": f.get("category"),
                          "family": f.get("family")}
    return out


def reachable_header(header):
    """Whether generated code can reach a symbol declared in `header`.

    An operator calls its MEOS entry by including the umbrella header the
    catalog names for it. A symbol whose only declaration is an INTERNAL header
    has no include a consumer may take, so it cannot be called at all, whatever
    its api field says -- `nad_tpointcloud_tpointcloud` is public and is
    declared in `tpc_boxops.h`, and the operator emitted for it named a symbol
    the translation unit could not see.
    """
    return bool(header) and bool(_UMBRELLA.fullmatch(header)) and "internal" not in header


def structural_residue(fn, ret, args, facts):
    """The residue reading no shape may override, or None.

    What a function IS -- unreachable, an aggregate transition, a value's own
    text form, an answer returned through its argument list -- decides that no
    per-event operator exists for it, whatever a shape makes of its signature.
    So this is asked BEFORE the shapes: asking it only where nothing matched
    lets a shape claim an out-parameter or an aggregate's state transition and
    the ledger then reads GENERATED for code no stream can run.
    """
    if not reachable_header(facts["file"]):
        return "RESIDUE:internal-header"
    category = facts["category"]
    if category in ("io", "constructor", "lifecycle", "index", "aggregate"):
        return f"RESIDUE:{category}"
    # A pointer-to-pointer or a scalar out-parameter returns through its
    # argument list, which no per-event operator signature can express.
    if any(a.endswith("**") for a in args):
        return "RESIDUE:out-param-array"
    if ret in ("void", "") and any(a.endswith("*") for a in args):
        return "RESIDUE:out-param-scalar"
    if "Datum" in ret or any("Datum" in a for a in args):
        return "RESIDUE:datum-internal"
    if trailing_out_param(fn, ret, args):
        # The function answers a PAIR: a flag saying whether it found anything,
        # and the value written through the last parameter. GoMEOS hands both
        # back -- `func STBOXTmax(box *STBox) (bool, int64)` -- and a per-event
        # operator yields ONE field, so it can carry the flag or the value and
        # not both. Returning the value alone would read "absent" as zero.
        return "RESIDUE:out-param-pair"
    return None


def _residue_reason(fn, ret, args, facts):
    """Why this public function carries no per-event streamable operator.

    Returns a reason string, or None when the function IS a candidate and its
    absence from the generated set is a GAP the generator owes rather than a
    property of the function.

    A category outside the streamable set is DEFERRED rather than residue: it
    says no shape reaches this family YET, which a new shape is free to answer,
    so it is read AFTER the shapes while `structural_residue` is read before.
    """
    structural = structural_residue(fn, ret, args, facts)
    if structural:
        return structural
    if facts["category"] not in _STREAMABLE_CATEGORIES:
        return f"DEFERRED:category-{facts['category']}"
    return None


def write_ledger(path, catalog, shapes):
    """The complete partition of the public surface, one line per function.

    Committing the ledger rather than the emitted operators is what makes a
    newly surfaced MEOS function impossible to drop silently: an operator the
    generator covers moves to GENERATED and the diff shows it, and one nothing
    covers appears as a new GAP line and fails the drift check.
    """
    sigs = catalog_sigs(catalog)
    facts = catalog_candidates(catalog)
    rows, tally = [], collections.Counter()
    for fn in sorted(facts):
        ret, args = sigs.get(fn, ("", ()))
        bucket = None
        structural = structural_residue(fn, ret, args, facts[fn])
        if structural:
            # what the function IS settles it, so no shape may claim it
            rows.append(f"{structural}\t{fn}")
            tally[structural.split(":")[0]] += 1
            continue
        for name, cls in shapes:
            try:
                if cls(fn, ret, args):
                    bucket = f"GENERATED:{name}"
                    break
            except Exception:
                continue
        if bucket is None:
            bucket = _residue_reason(fn, ret, args, facts[fn]) or "GAP"
        rows.append(f"{bucket}\t{fn}")
        tally[bucket.split(":")[0]] += 1
    with open(path, "w") as fh:
        fh.write("# MobilityNebula coverage ledger — every public MEOS function in exactly\n")
        fh.write("# one bucket. Regenerate with build_descriptor.py --catalog <idl> --ledger.\n")
        for r in rows:
            fh.write(r + "\n")
    return tally, len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sigs", help="header signature dump (legacy; --catalog supersedes it)")
    ap.add_argument("--gap", help="streamable-not-wired list (legacy; --catalog supersedes it)")
    ap.add_argument("--shapes", required=True,
                    help="comma-separated SHAPE names to apply (order = match priority)")
    ap.add_argument("--out", help="descriptor JSON output path")
    ap.add_argument("--catalog", default=None,
                    help="composed meos-idl.json; the canonical single input — signatures "
                         "and the candidate surface are both read from it")
    ap.add_argument("--ledger", default=None,
                    help="write the complete public-surface partition to this path")
    a = ap.parse_args()

    if not a.catalog and not (a.sigs and a.gap):
        ap.error("--catalog is required (or the legacy --sigs with --gap)")
    if not a.out and not a.ledger:
        ap.error("nothing to write: pass --out, --ledger, or both")

    if a.catalog:
        load_catalog(a.catalog)
        sys.stderr.write(f"catalog: loaded {len(_CATALOG)} sqlfn entries from {a.catalog}\n")

    shape_names = a.shapes.split(",")
    if a.ledger:
        tally, total = write_ledger(a.ledger, a.catalog,
                                    [(n, SHAPES[n]) for n in shape_names])
        sys.stderr.write(f"ledger: {total} public function(s) -> {a.ledger}\n")
        for k in sorted(tally):
            sys.stderr.write(f"   {k:<12}{tally[k]}\n")
        if not a.out:
            return

    if a.catalog:
        sigs = catalog_sigs(a.catalog)
        facts = catalog_candidates(a.catalog)
        # The SAME structural gate the ledger applies, so what the ledger calls
        # GENERATED and what the descriptor carries are one set: a gate on only
        # one of the two makes the ledger a claim about code the emitter does
        # not write, or writes for a function the ledger calls residue.
        gap = {fn for fn, f in facts.items()
               if not structural_residue(fn, *sigs.get(fn, ("", ())), f)}
    else:
        sigs = parse_sigs(a.sigs)
        gap = {ln.strip() for ln in open(a.gap) if ln.strip()}
    shapes = [SHAPES[s] for s in shape_names]

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

    # the operand builder travels WITH the operator, so the emitter needs no
    # table of how each temporal type is constructed
    for d in ops:
        spec = _DERIVED_INPUTS.get(d.get("input_type"))
        if spec:
            d["input_spec"] = spec

    json.dump({"_comment": f"codegen descriptor; shapes={a.shapes}", "operators": ops},
              open(a.out, "w"), indent=2)
    sys.stderr.write(f"emitted {len(ops)} operator descriptor(s) -> {a.out}\n")
    sys.stderr.write(f"(gap functions present in sig-dump but unmatched by these shapes: "
                     f"{len([f for f in unmatched if f in sigs])})\n")


if __name__ == "__main__":
    main()
