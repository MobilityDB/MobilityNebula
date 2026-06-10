# On integrating a C library (MEOS) into NebulaStream — friction & proposals

**Context for the NebulaStream team.** We are exposing [MEOS](https://libmeos.org)
(the Mobility Engine, Open Source — the C core of MobilityDB) as first-class
streaming operators in NebulaStream, so that spatiotemporal queries (`tfloat`,
`tgeompoint`, `STBox`, `Span`, …) run natively in a streaming job. MEOS exports
~1,900 streaming-relevant C functions. This note explains why wiring **one**
exported C function into NebulaStream currently costs **~8 artifacts**, contrasts
it with a "normal" library, and proposes ways to make it cheap.

## The core problem: one C function → ~8 NebulaStream artifacts

A scalar C function is a one-liner to *call*:

```c
extern double geog_length(const GSERIALIZED *g, bool use_spheroid);
```

To make it callable from a NebulaStream SQL query, today we must generate **all
of the following** (we automate it with a codegen, but the surface is the point):

1. `GeogLengthLogicalFunction.hpp` — logical operator declaration
2. `GeogLengthLogicalFunction.cpp` — arity/type checking, registry glue
3. `GeogLengthPhysicalFunction.hpp` — physical operator declaration
4. `GeogLengthPhysicalFunction.cpp` — the Nautilus `invoke` that builds the MEOS
   value from event fields, calls `geog_length`, and serializes the result
5. **Grammar lexer token** in `AntlrSQL.g4`: `GEOG_LENGTH`
6. **Grammar `functionName` alternative** in the same `.g4` (one giant rule)
7. **Parser dispatch case** in `AntlrSQLQueryPlanCreator.cpp`
8. Two `CMakeLists.txt` registrations (logical + physical) + a systest

So **8 files / insertion points per function × ~1,900 functions**. The grammar
and parser are *global* shared files, so every function also edits a
1,900-token single-line ANTLR rule and a giant dispatch switch — a serialization
bottleneck and a constant merge-conflict surface.

## Why this is worse than a "normal" library (e.g. a JSON parser)

Integrating, say, `nlohmann::json` or RapidJSON into an engine is cheap because:

- It exposes a **handful of generic entry points** (`parse`, `dump`, `operator[]`,
  type queries). You bind ~5 methods and you have the whole library.
- Its types are **self-describing containers** (objects/arrays/scalars) — the
  engine needs one `JSON` logical type, not one per JSON shape.
- There is **no per-operation grammar**: JSON access is `json_extract(col, '$.a')`,
  a single function whose behavior is data-driven by an argument.

MEOS is the opposite, and that's inherent to the domain, not a defect:

- It is **wide, not deep**: ~1,900 *distinct typed operations* (`edwithin_tgeo_geo`,
  `tfloat_value_split`, `temporal_basetype`, …), each a separate semantic. There
  is no generic "do MEOS" entry point.
- Its values are **opaque heap structs** (`Temporal*`, `GSERIALIZED*`, `Span*`)
  with MEOS-owned allocation — every operator must marshal event fields → a MEOS
  value, call, serialize back, and free, respecting MEOS's allocator.
- The **type drives the function**: `tfloat_value_split` vs `tint_value_split` are
  different exported symbols, so a single SQL surface can't cover them without
  per-type wiring.

A concrete multiplicity example: the single user concept "value at a timestamp"
is **7 exported C symbols** — `tfloat_value_at_timestamptz`,
`tint_value_at_timestamptz`, `tbool_…`, `ttext_…`, `tgeo_…`, `tpose_…`,
`temporal_at_timestamptz` — because the return type differs. Each becomes its own
8-artifact operator. The same fan-out hits hashing (8 `*_hash_extended`),
type predicates (43 `*_type`/`*_basetype`), splits, boxes, bins, etc.

## What makes it *especially* expensive in NebulaStream specifically

1. **The grammar is the registry.** A function isn't callable until it has a
   lexer token + a `functionName` alternative + a parser case. That couples
   "library coverage" to "grammar edits," which is the part that doesn't scale:
   the `functionName` rule is one line holding all tokens; per-function edits to
   it don't compose and can't be parallelized.
2. **No data-driven dispatch.** There's no way to say `meos_call('geog_length',
   args...)` and resolve the symbol + signature at plan time. Every symbol needs
   a compiled operator, so the binary grows with the API.
3. **Aggregation vs per-event is a hard fork.** A function registered as an
   aggregation can't also be used windowless (it throws 9005 "Only
   TimeBasedWindowType"). Some MEOS functions are legitimately both; today that's
   a duplicate-implementation decision, not a runtime choice.

## Proposals to make C-library integration cheap (in rough priority)

1. **A generic UDF-by-symbol path.** Allow a function to be invoked as
   `MEOS('geog_length', col, true)` (or via a registered descriptor) where the
   engine looks up an (arg-types → ret-type) descriptor at plan time and a
   single generic physical operator marshals/calls/serializes by descriptor.
   This collapses ~1,900 operators into **1 operator + a descriptor table** (a
   JSON/registry file), and removes the grammar from the per-function path
   entirely. This is the single highest-leverage change.
2. **Grammar-free function registration.** Decouple "callable function" from "new
   grammar token": resolve unknown `IDENTIFIER(args)` calls against a function
   registry instead of requiring a `functionName` alternative per name. Then
   adding a function is a registry entry, not a `.g4` edit — and the
   merge-conflict / single-line-rule problem disappears.
3. **A typed external-value handle.** A first-class `OpaqueExternal<tag>` logical
   type wrapping a `void*` + a vtable (serialize/free/typecode), so MEOS values
   flow between operators without each operator re-parsing WKT/hex. This makes
   chained MEOS ops (the common case) cheap and removes per-operator marshalling.
4. **Descriptor-generated operators (if (1) is too big a step).** Standardize the
   physical-operator template so it's filled purely from a per-function
   descriptor `(name, arg_kinds, ret_kind, headers)` — which is essentially what
   our codegen already does. Upstreaming that template + descriptor format would
   let any C library be onboarded by emitting a descriptor table, not C++.
5. **Runtime aggregation/per-event duality.** Let one registered function be used
   either windowless (per-event) or in a window (aggregation), chosen by the
   query, instead of a compile-time fork — removes the 9005 class of problems and
   the duplicate operators.

**Bottom line:** the cost isn't MEOS being unusual C — it's that NebulaStream
today requires a *compiled, grammar-registered operator per exported symbol*. A
**generic symbol-dispatch UDF + a function registry that doesn't go through the
grammar** would turn "integrate a C library" from ~8 artifacts × N functions into
"point the engine at a descriptor table," and would generalize far beyond MEOS.
