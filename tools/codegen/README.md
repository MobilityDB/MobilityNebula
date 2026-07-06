# MobilityNebula MEOS-operator codegen — design + generator

This directory contains the design proposal and Python generator for
scaling MobilityNebula's MEOS-operator surface from the current
~17 hand-written operators (PRs #14, #15, #16, #17) to a larger
fraction of MEOS' ~1,949 streamable public functions, mirroring the
infrastructure parity that the Flink and Kafka platforms reached via
their codegen + wirings stacks.

## Why codegen on Nebula

The streaming-platform parity audit
([assessment](../../docs/berlinmod-streaming-forms.md)) shows:

| Platform | Wirable MEOS surface |
|---|---:|
| Flink | 2,097 / 2,097 (100%) via codegen + 5 generic wiring classes |
| Kafka | 2,097 / 2,097 (100%) via codegen + 5 generic wiring classes |
| **Nebula** | **~17 / 2,097 (~1%)** via hand-written 4-layer pipeline per function |

The Nebula gap is structural: each MEOS function on NebulaStream
requires a full **4-layer pipeline tuple** — logical class, physical
class, parser dispatch, lowering rule — totalling ~350–400 LOC of
mostly-mechanical boilerplate per function. Hand-writing all of MEOS'
streamable surface this way is multi-month engineering; codegen makes
it tractable.

## What this codegen produces

For each MEOS scalar function `f` in the input list, the generator
emits the four NebulaStream pipeline-layer files following the
established style of the existing hand-written operators
(`TemporalEDWithinGeometryLogicalFunction` etc.):

```
nes-logical-operators/include/Functions/Meos/<NebulaName>LogicalFunction.hpp
nes-logical-operators/src/Functions/Meos/<NebulaName>LogicalFunction.cpp
nes-physical-operators/include/Functions/Meos/<NebulaName>PhysicalFunction.hpp
nes-physical-operators/src/Functions/Meos/<NebulaName>PhysicalFunction.cpp
```

Plus updates to:
- `nes-logical-operators/src/Functions/Meos/CMakeLists.txt`
- `nes-physical-operators/src/Functions/Meos/CMakeLists.txt`
- Parser dispatch: a single block per generated function inserted into
  `nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp` (manual edit
  recommended; the generator emits the dispatch snippet for
  copy-paste)
- Parser grammar: a single token per function added to
  `nes-sql-parser/AntlrSQL.g4` (same)

## Scope of this PR

**Generator infrastructure only.** No generated C++ committed. Reasons:

1. **Compile-environment constraint.** The generator's author cannot
   build NebulaStream (full C++23 + vcpkg toolchain). Committing
   unverified generated code would ship potentially broken operators.
2. **Per-function review value.** Mariana (maintainer) can run the
   generator against a small input list (e.g. one MEOS family at a
   time), review the output, iterate on the templates if needed, and
   ship operators in follow-up PRs at a controlled pace.
3. **Template iteration cost.** First-pass templates may need
   adjustment after the first build — better to land the generator
   and iterate on templates than to ship a large batch of generated
   operators that all have the same wrong shape.

## How to use the generator

```bash
# Edit the input list to choose which MEOS functions to generate
$EDITOR tools/codegen/codegen_input.example.json

# Run the generator
python3 tools/codegen/codegen_nebula.py \
    --input tools/codegen/codegen_input.example.json \
    --output-root .

# Output:
#   nes-logical-operators/include/Functions/Meos/<NebulaName>LogicalFunction.hpp
#   nes-logical-operators/src/Functions/Meos/<NebulaName>LogicalFunction.cpp
#   nes-physical-operators/include/Functions/Meos/<NebulaName>PhysicalFunction.hpp
#   nes-physical-operators/src/Functions/Meos/<NebulaName>PhysicalFunction.cpp
#
# Plus a stderr-printed "parser snippet" per function that you paste into
# nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp (the parser dispatch),
# and a "grammar snippet" that you paste into AntlrSQL.g4
```

## Input format

`codegen_input.example.json` is a list of MEOS-function descriptors.
One descriptor per output operator:

```json
{
  "operators": [
    {
      "nebula_name": "TemporalEDisjointGeometry",
      "sql_token":  "TEMPORAL_EDISJOINT_GEOMETRY",
      "meos_call":  "edisjoint_tgeo_geo",
      "args": [
        {"name": "lon",      "nautilus_type": "double",   "cpp_type": "double"},
        {"name": "lat",      "nautilus_type": "double",   "cpp_type": "double"},
        {"name": "timestamp","nautilus_type": "uint64_t", "cpp_type": "uint64_t"},
        {"name": "geometry", "nautilus_type": "VariableSizedData", "cpp_type": "const char*"}
      ],
      "return_type":     "int",
      "nautilus_return": "INT32",
      "build_temporal_point": true,
      "comment_one_liner": "Per-event ever-disjoint between a tgeompoint built from event fields and a static geometry."
    }
  ]
}
```

Field meanings:
- `nebula_name`: PascalCase NebulaStream class name (without `LogicalFunction` / `PhysicalFunction` suffix; the generator adds those)
- `sql_token`: the uppercase SQL function name (Antlr lexer token)
- `meos_call`: the underlying MEOS C function symbol the physical operator wraps
- `args`: ordered list of per-record argument fields; the generator builds the constructor + `parameters` vector from these
- `return_type` / `nautilus_return`: the MEOS function's C return type and the NebulaStream `DataType::Type` enum value
- `build_temporal_point`: if true, the physical operator builds a single-instant tgeompoint from `(lon, lat, timestamp)` before calling MEOS (the common pattern for spatial predicates); if false, the operator passes args directly to MEOS
- `comment_one_liner`: drops into the Javadoc-equivalent C++ doc comment

## Templates

The generator's templates are embedded in the Python source as
multi-line f-strings. They mirror the exact layout of the existing
hand-written operators (`TemporalEDWithinGeometryLogicalFunction` and
its physical sibling are the reference; the templates were derived by
1:1 inspection of those files).

To adjust a template (e.g. when NebulaStream's `LogicalFunctionConcept`
adds a new override), edit the corresponding string in
`codegen_nebula.py`; the change applies to all subsequent
regenerations.

## Scaling path (recommended sequence)

| Wave | Scope | Expected output | Effort estimate |
|---|---|---|---|
| W1 | First batch: 5 MEOS spatial-relation E/A predicates (e.g. `TemporalEDisjoint`, `TemporalATouches`, `TemporalECovers`, `TemporalACrosses`, `TemporalAOverlaps`) | 20 generated files + 5 parser entries | Single follow-up PR after this generator lands |
| W2 | All ever / always spatial-relation predicates over `tgeo_geo` (~18 functions) | 72 generated files | ~1 follow-up PR |
| W3 | Distance functions over `tgeo_geo` and `tgeo_tgeo` (NAD, NAI, distance, etc.) | ~30 generated files | ~1 follow-up PR |
| W4 | Scalar accessors that decompose to per-event reads | template extension required (read MEOS handle) | design decision point |
| W5 | Aggregations (windowed / cross-stream) | separate generator (aggregation 4-layer pattern is different from scalar 4-layer pattern; the existing TEMPORAL_LENGTH / PAIR_MEETING / CROSS_DISTANCE shape) | full aggregation-codegen design |

Per-PR scope keeps the review surface small and lets each batch land
with its own build verification.

## What the generator does NOT do (deliberately)

- **No build-system integration.** The CMakeLists updates are emitted
  as text snippets for the maintainer to apply manually. This avoids
  the generator silently corrupting CMakeLists on regeneration.
- **No parser/grammar integration.** Same reason — the dispatch and
  grammar snippets are emitted to stderr for manual paste.
- **No aggregation-pattern support yet.** Aggregations require a
  different 4-layer shape (lift/combine/lower/cleanup) that depends
  on per-aggregation state design. A separate generator with the
  aggregation-specific template is W5 in the table above.

## Compile-verification note

The generator's first output should be reviewed against an existing
hand-written operator for shape parity, then `mvn compile` (or the
NebulaStream `cmake --build` equivalent) should be run against a
single small batch (1–2 generated functions) before scaling up. The
generator's templates are derived 1:1 from the existing operator
shape but have not been compile-tested in this PR (out of the
generator author's environment).
