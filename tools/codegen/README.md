# tools/codegen — IDL-driven MEOS operator generator for NebulaStream

This directory contains the Python generator that produces the four-layer
NebulaStream pipeline tuple (logical .hpp/.cpp + physical .hpp/.cpp) for every
MEOS scalar function in a descriptor, and idempotently patches the CMakeLists,
Antlr grammar, and QPC dispatch so the generated operators build and parse
without manual edits.

## Pipeline overview

```
MEOS headers (pinned)
        │
        ▼
  MEOS-API run.py          # libclang-based extractor (lives in MEOS-API repo)
        │  produces meos-idl.json (name + signature authority; NOT committed here)
        ▼
  build_descriptor.py      # classifies gap functions into known operator SHAPEs
        │  --sigs <header-dump> --gap <gap-list> --shapes <shape,...> --out <descriptor.json>
        │  produces e.g. trgeo-descriptor.json
        ▼
  codegen_nebula.py        # emits C++ + patches build/grammar/QPC
        │  --input <descriptor.json> --output-root <MobilityNebula-root>
        │
        ├── nes-logical-operators/include/Functions/Meos/<Name>LogicalFunction.hpp
        ├── nes-logical-operators/src/Functions/Meos/<Name>LogicalFunction.cpp
        ├── nes-physical-operators/include/Functions/Meos/<Name>PhysicalFunction.hpp
        ├── nes-physical-operators/src/Functions/Meos/<Name>PhysicalFunction.cpp
        │
        ├── (idempotent) nes-{logical,physical}-operators/src/Functions/Meos/CMakeLists.txt
        ├── (idempotent) nes-sql-parser/AntlrSQL.g4 — lexer token + functionName rule
        └── (idempotent) nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp — dispatch case
```

The IDL (`meos-idl.json`) is a 2 MB generated artifact; regenerate it via
`python3 run.py <pinned-meos-headers>` in the MEOS-API repo. It is the
name/signature authority for all downstream ecosystem bindings and is never
committed here.

## Regenerating the IDL

```bash
# In the MEOS-API repo, against the pinned MEOS headers:
python3 run.py /usr/local/include/meos.h /usr/local/include/meos_geo.h \
    > /tmp/meos-idl.json
```

## Running the generator

```bash
# 1. Build a descriptor from the IDL gap (example: trgeometry family)
python3 tools/codegen/build_descriptor.py \
    --sigs /tmp/cmp_sigs.txt \
    --gap  /tmp/nebula_gap.txt \
    --shapes trgeometry_geo_predicate,geo_trgeometry_predicate,trgeometry_geo_dwithin,\
trgeometry_trgeometry_predicate,trgeometry_trgeometry_dwithin,trgeometry_nad \
    --out tools/codegen/trgeo-descriptor.json

# 2. Emit C++ files and patch build/grammar/QPC
python3 tools/codegen/codegen_nebula.py \
    --input tools/codegen/trgeo-descriptor.json \
    --output-root .

# Optional flags:
#   --no-parser-glue    skip AntlrSQL.g4 + AntlrSQLQueryPlanCreator.cpp injection
#   --no-cmake-entries  skip CMakeLists.txt injection
```

Both injectors are idempotent: per-op markers
(`/* BEGIN CODEGEN PARSER GLUE: <TOKEN> */ … /* END CODEGEN PARSER GLUE */`)
gate each block, and the script skips insertion when the marker is already
present. The physical CMakeLists places `add_plugin` entries inside the
`if(NES_ENABLE_MEOS) … endif()` guard.

## Operator SHAPEs (build_descriptor.py classifiers)

Each SHAPE classifier matches a class of MEOS function signatures and
synthesises the per-event SQL argument list and the `build_*` flag that
selects the corresponding physical C++ template in `codegen_nebula.py`.

| build_* flag | Per-event SQL args | Typical MEOS signature |
|---|---|---|
| `build_temporal_point` | lon, lat, ts, geometry | `int fn(Temporal*, GSERIALIZED*)` |
| `build_temporal_point_with_dist` | lon, lat, ts, geometry, dist | `int fn(Temporal*, GSERIALIZED*, double)` |
| `build_tgeo_tgeo` | lonA, latA, tsA, lonB, latB, tsB | `int fn(Temporal*, Temporal*)` |
| `build_tgeo_tgeo_with_dist` | lonA, latA, tsA, lonB, latB, tsB, dist | `int fn(Temporal*, Temporal*, double)` |
| `build_trgeometry_geo` | ref_wkt, x1, y1, theta1, ts1, tgt_wkt | `int fn(Trgeometry*, GSERIALIZED*)` |
| `build_geo_trgeometry` | tgt_wkt, ref_wkt, x1, y1, theta1, ts1 | `int fn(GSERIALIZED*, Trgeometry*)` |
| `build_trgeometry_geo_with_dist` | ref_wkt, x1, y1, theta1, ts1, tgt_wkt, dist | `int fn(Trgeometry*, GSERIALIZED*, double)` |
| `build_trgeometry_trgeometry` | ref1_wkt, x1, y1, theta1, ts1, ref2_wkt, x2, y2, theta2, ts2 | `int fn(Trgeometry*, Trgeometry*)` |
| `build_trgeometry_trgeometry_with_dist` | + dist (11-arg) | `int fn(Trgeometry*, Trgeometry*, double)` |
| `build_tnumber_point_with_scalar` | value, ts, scalar | `int fn(Temporal*, double\|int)` |
| `build_generic` | arbitrary typed fields | any MEOS scalar, via GENERIC_RETURNS map |

## C-to-NES type map (GENERIC_RETURNS)

| MEOS C return | nautilus_return | C++ return | zero literal |
|---|---|---|---|
| `int` | `INT32` | `int` | `0` |
| `double` | `FLOAT64` | `double` | `0.0` |
| `bool` | `BOOLEAN` | `bool` | `false` |
| `int` (from `Temporal*` via accessor) | `INT32` | `int` | `0` |
| `double` (from `Temporal*` via accessor) | `FLOAT64` | `double` | `0.0` |
| `VariableSizedData` (hex-WKB) | `VARSIZED` | `VariableSizedData` | `nullptr` |

Per-event field types follow the same mapping: `double` -> `nautilus::val<double>`,
`uint64_t` -> `nautilus::val<uint64_t>`, `VariableSizedData` -> pointer+size pair.

## Descriptor format

`codegen_input.example.json` shows a minimal descriptor.
`trgeo-descriptor.json` shows the full trgeometry family (80 operators).

Key fields:

| Field | Meaning |
|---|---|
| `nebula_name` | PascalCase class name prefix (generator adds `LogicalFunction` / `PhysicalFunction`) |
| `sql_token` | UPPER_SNAKE_CASE Antlr lexer token and SQL function name |
| `meos_call` | Underlying MEOS C symbol |
| `return_type` | C return type (`int`, `double`, `bool`) |
| `nautilus_return` | NES `DataType::Type` enum value |
| `build_*` | SHAPE flag selecting the physical C++ template |
| `comment_one_liner` | Drops into the C++ doc comment |

## Self-test (reproducibility gate)

Run the generator against `trgeo-descriptor.json` at the W147 baseline
(commit `ac7e7d1469`, the clean state before any trgeometry files existed)
and diff the output against the committed trgeometry files at W149
(`fork/feat/nebula-codegen-w149-trgeometry-trgeometry-predicates`,
commit `6b2180c089`). All 80 per-op files must be byte-identical.

```bash
git worktree add /tmp/nebgen-selftest ac7e7d1469
cp -r tools/codegen /tmp/nebgen-selftest/tools/
cd /tmp/nebgen-selftest
python3 tools/codegen/codegen_nebula.py \
    --input tools/codegen/trgeo-descriptor.json \
    --output-root . \
    --no-cmake-entries --no-parser-glue
# diff against W149 committed files
git diff 6b2180c089 -- 'nes-*/include/Functions/Meos/*Trgeometry*' \
                       'nes-*/src/Functions/Meos/*Trgeometry*'
git worktree remove /tmp/nebgen-selftest
```

## Local compile check

`build_local.sh` drives the NebulaStream dev image build for the three
operator/parser libraries without requiring a host C++23 toolchain.
It temporarily disables the optional MQTT plugins (absent in the dev image)
and restores `nes-plugins/CMakeLists.txt` on exit via a `trap`.

```bash
tools/codegen/build_local.sh                         # default: nes-physical-operators nes-logical-operators nes-sql-parser
NES_DEV_IMAGE=localhost/nes-development:my-tag \
    BUILD_DIR=build-custom \
    tools/codegen/build_local.sh nes-physical-operators
```

## Aggregation operators

`codegen_aggregations.py` handles the separate aggregation four-layer shape
(lift / combine / lower / cleanup), whose per-op structure differs from the
scalar template above. Run it the same way, with a descriptor whose operators
carry aggregation-specific fields.
