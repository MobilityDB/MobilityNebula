# Streaming-parity assessment — the 3 platforms, measured

Result of the [streaming-parity methodology](./streaming_parity.md) across the
three streaming platforms, each over its accumulated-PR build. The reference
surface is the **1,945** streamable MEOS public functions (tiers
`stateless`/`bounded-state`/`windowed`/`cross-stream`).

| Platform | **L3 CALLABLE** (binding invokes it, confirmed) | L2 wired-only (registered, not yet confirmed callable) | gap (streamable, not wired) |
|---|---|---|---|
| **NebulaStream** | **59 — 3.0%** | 335 — 17.2% | 1,551 — 79.7% |
| **Flink** | **1,945 — 100.0%** | 0 — 0.0% | 0 — 0.0% |
| **Kafka** | **1,945 — 100.0%** | 0 — 0.0% | 0 — 0.0% |

> L3 is **callability** — the binding actually invoked the function on real
> `libmeos` (correctness is inherited from MEOS's PostgreSQL suite; see the
> methodology's two-tier model). Flink/Kafka share the generated JNR-FFI facade,
> so their callability is identical. It is confirmed by a **type-aware per-method
> callability run**: each facade method is invoked in its own JVM (return /
> caught MEOS semantic error / native exit / abort = callable; only a
> linkage/marshalling exception = not-callable), with each `Pointer` argument
> built from a per-type sample inferred from the function-name tokens (the
> "connectors": `acontains_geo_tgeo` → [geometry, tgeompoint],
> `tdwithin_tgeo_tgeo` → [tgeompoint, tgeompoint]).

The headline measures callability, not facade size: a "2,097 wirable" count is
the L2 facade size, not what runs. The full residual is attributed by `nm -D`
and facade introspection, not estimated.

## libmeos must be pinned

JMEOS loads the library via `LibraryLoader.search(<user.dir>/src).load("meos")`,
which resolves through the OS loader and **ignores `-Djnr.ffi.library.path`**.
The harness therefore pins the libmeos under test on `LD_LIBRARY_PATH`
(`run_callability.sh` does this); without it the OS loader can resolve a stale
`/usr/local/lib/libmeos.so` instead of the build under test. The measurement
here uses the `accumulate/parity-1.4` libmeos.

## Flink / Kafka — 100% confirmed callable, 0 residual

Every one of the 1,945 streamable functions has a facade method and is confirmed
callable on real `libmeos` — no wired-only, no gap. The harness builds each
input from the function-name tokens, with native `T**` arrays
(`Memory.allocateDirect`) for set/array constructors, a `trgeometryinst_make`
sample for the `trgeometry` family, `Interval`/`OffsetDateTime`/`LocalDateTime`
samples for the time functions, and an out-param buffer for the catalog/SRID
out-params. Four functions (`tfloat_avg_value`, `geog_from_binary`,
`srid_check_latlong`, `nad_stbox_trgeometry`) are not in the surface, which
totals 1,945.

## Reason-marked (NOT streamable, never gaps)

`io-meta` 218 · `sequence-only` 14 · `ambiguous` 59 · `internal` 1,308.

## How each layer is measured

| Platform | L2 WIRED | L3 CALLABLE |
|---|---|---|
| NebulaStream | operators' `meos_call` in `*PhysicalFunction.cpp` | systest tokens (resolved to the operator by normalized-name match) whose test passes end-to-end on a local worker |
| Flink / Kafka | `public static` methods of `MeosOps*.java` facade | facade methods the type-aware per-method callability harness invoked on real libmeos without a binding failure (`callability/PerMethodCallability.java` + `run_callability.sh::run_per_method`) |

Adapters: `tools/streaming_parity/adapters/{nebula.py, jvm_facade.py}`;
callability harness: `tools/streaming_parity/callability/`. The committed
`feeds/flink-kafka.feed.tsv` + `feeds/streamable.txt` reproduce the table via
`ci_gate.py` without re-running the harness.

## Running-aggregation realization (Flink/Kafka vs NebulaStream)

The scope is identical on every platform: the full MEOS API over a MEOS value
produced in the window (a per-group mini-trip trajectory). The MEOS operations
are the invariant C calls; only how the running aggregate's value is held and
the API is invoked differs, driven by each platform's state model.

- **MobilityFlink / MobilityKafka** hold the windowed value as **WKB in
  checkpointed, fault-tolerant managed state** — exactly-once recovery requires
  the state to be serializable. The MEOS library is the JMEOS facade
  (`MeosOps*` methods) over a native `Pointer`; the value is reconstructed from
  WKB (`from_wkb`) for `append_tinstant` and for each function call. WKB is
  mandatory here.
- **MobilityNebula** materializes the windowed value inside a physical
  aggregate operator and applies the MEOS library to it **directly**, in
  process — no per-window WKB serialization. MEOS's expandable structures
  (`count`/`maxcount`, `appendInstant`/`appendSequence`; the streaming design
  in the libmeos data-structures documentation) are the in-process accumulator
  for this model — amortized-O(1) in-place growth as each instant is appended.
  WKB appears only when a value crosses an operator boundary.

So WKB is the serialization/exchange form (mandatory in the JVM tools' state,
boundary-only in NebulaStream) and the expandable `Temporal*` is the in-memory
production form; both serve the one scope.

## Status

- **Flink / Kafka: 100% PROVEN** (1,945 / 1,945 confirmed callable on real
  libmeos). The CI gate (`ci_gate.py` + `.github/workflows/streaming_parity_gate.yml`)
  holds the floor at 1,945 and blocks any regression or over-claim; the committed
  feed reproduces it without re-running the harness.
- **NebulaStream: 394 / 1,945 wired and locally compile-verified.** The
  generated `nes-{physical,logical}-operators` + `nes-sql-parser` libraries link
  clean in the `nebulastream/nes-development` dev image against the `libmeos`
  under test; 59 are confirmed callable via systests that run end-to-end against
  a local single-node worker (query plan serialized, deserialized, compiled, and
  executed; result matched against the value a faithful MEOS probe produces). The wired surface
  spans per-event operators over the tgeompoint/tcbuffer/tpose/tnumber families
  (comparison, spatial-relation, distance, scalar/extract/box-literal shapes, and
  position/topological predicates of a temporal against an `STBox`/`TBox` query
  literal in either argument order — `left`/`right`/`above`/`below`/`front`/`back`,
  `before`/`after`, the `over*` half-predicates, `adjacent`/`contains`/`contained`/`overlaps`/`same`),
  emitted by the signature-driven descriptor-builder
  (`tools/codegen/build_descriptor.py`), which classifies a gap function by its
  exact in-header signature so emission is measured-not-guessed. Every
  single-accumulator windowed aggregate holds one incremental MEOS accumulator in
  its `AggregationState` slot, folded per event in `lift()` (O(1) state, no event
  buffer) and merged in `combine()` — the bounding extent box for `TSPATIAL_EXTENT`
  (`STBox` via `tspatial_extent_transfn` → `stbox_out`, merge `union_stbox_stbox`)
  and `TNUMBER_EXTENT` (`TBox`, `union_tbox_tbox`); the value/time `Span` for
  `FLOAT_EXTENT` / `INT_EXTENT` / `BIGINT_EXTENT` / `TIMESTAMPTZ_EXTENT`
  (`*_extent_transfn` → typed `*span_out`, merge `super_union_span_span`); and the
  deduplicated `Set` for `FLOAT_UNION` / `INT_UNION` / `BIGINT_UNION` /
  `TIMESTAMPTZ_UNION` (`*_union_transfn` → `set_union_finalfn` → typed `*set_out`,
  merge `set_union_transfn`). The result is serialized to text/VARSIZED in `lower()`.
  Windowed value-output aggregates grow the per-group mini-trip on the in-process
  expandable `Temporal*` (`appendInstant`) and emit a MEOS function over it as
  hex-WKB — `TRAJECTORY_WKB` (the materialized trajectory), `TLENGTH_EXP`,
  `TEMPORAL_COPY_EXP`, `TNUMBER_ABS_EXP`, and the single-argument temporal
  transforms `TPOINT_CUMULATIVE_LENGTH_EXP` / `TPOINT_SPEED_EXP` /
  `TPOINT_GET_X_EXP` / `TPOINT_GET_Y_EXP` (tgeompoint → tfloat) and
  `TNUMBER_TREND_EXP` (tnumber → tint). The geometry value-output family reduces
  the mini-trip to a `GSERIALIZED` serialized as hex-EWKB via `geo_out` —
  `TGEO_START_VALUE_EXP` / `TGEO_END_VALUE_EXP` / `TGEO_CONVEX_HULL_EXP` /
  `TPOINT_TWCENTROID_EXP`.
  The network-constrained `tnpoint` aggregates run the same way over a windowed
  npoint mini-series, resolving each route+fraction against the loaded ways
  network: `TNPOINT_CUMULATIVE_LENGTH_EXP` (`tnpoint_cumulative_length`),
  `TNPOINT_SPEED_EXP` (`tnpoint_speed`), and the dedicated conversion
  `TNPOINT_TO_TGEOMPOINT_EXP` (`tnpoint_to_tgeompoint`, the network-resolved
  spatial trajectory). Each operator carries a systest
  (`nes-systests/function/meos/`) that exercises it end-to-end and rides Nebula
  CI's address/undefined/thread sanitizer matrix as a per-operator memory-leak
  gate.
  - **Cross-stream (pairwise).** Observations group per vehicle, so a cross-vehicle
    alert is the per-vehicle windowed aggregate composed with a cross-vehicle
    comparison: `GROUP BY vehicle_id ... TSPATIAL_EXTENT(lon, lat, ts)` gives each
    vehicle its extent `STBox`, and a self-join of that per-vehicle box stream with
    the per-event predicate `overlaps_stbox_stbox(boxA, boxB)` raises the SNCB
    box-overlap alert. The 29 `STBox`-vs-`STBox` cross-vehicle functions are wired
    as per-event operators taking two `STBox` VARSIZED inputs (each `stbox_in`,
    freed) — the topological/position predicates (overlaps/contains/contained/
    left/right/above/below/front/back/adjacent/same + `over*` + `nad`) and the
    comparators (`stbox_eq/ne/lt/le/gt/ge`, `stbox_cmp`), via `codegen_nebula`'s
    `stbox_text` input + the `stbox_x_stbox` classifier. The 13 `tnumber`-vs-`tnumber`
    position predicates (adjacent/after/before/contained/contains/left/right/`over*`/
    same) are wired the same way over two hex-WKB temporal operands (each
    `temporal_from_hexwkb`, freed) — `codegen_nebula`'s `wkb_temporal` extra-arg +
    the `two_temporal_scalar` classifier — the value/time counterpart of the STBox
    predicates. The `tnpoint`-vs-`tnpoint` and `tpose`-vs-`tpose` value-comparison
    predicates ride the same `two_temporal_scalar` path (its `extra_headers` pulls
    `meos_npoint.h` / `meos_pose.h`): the ever/always equality tests
    (`always_eq`/`always_ne`/`ever_eq`/`ever_ne`, `int` result over two
    per-vehicle mini-trajectories) and the network-/pose-resolved nearest-approach
    distance `nad_tnpoint_tnpoint` / `nad_tpose_tpose` — so two trains' route-bound
    or pose-bound windows compare directly. The overall (all-vehicles) view is a
    derivation over the per-vehicle aggregates, not a separate aggregate; `PAIR_MEETING` (`geog_dwithin`) and
    `CROSS_DISTANCE` (`nad_tgeo_tgeo`) are the BerlinMOD-scaffold single-aggregate
    convenience (one window over all vehicles, pairwise enumeration in `lower()`).
  - Not wired: the Set/Span/Box-input aggregation band, and the cross-vehicle
    `f(trajA, trajB)` family that returns a `Temporal*` rather than a scalar —
    `tdistance_tgeo_tgeo`, `tcontains/tcovers/tdwithin_tgeo_tgeo`,
    `add/sub/mul/div_tnumber_tnumber` (27), `TInstant*` `nai_*` (4), and
    `GSERIALIZED*` `shortestline_*` (4).
