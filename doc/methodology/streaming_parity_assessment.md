# Streaming-parity assessment — the 3 platforms, measured

Result of the [streaming-parity methodology](./streaming_parity.md) across the
three streaming platforms, each from its committed feed in
`tools/streaming_parity/feeds/`. The reference surface is the **1,938**
streamable MEOS public functions (tiers
`stateless`/`bounded-state`/`windowed`/`cross-stream`).

| Platform | **L3 CALLABLE** (binding invokes it, confirmed) | L2 wired-only (registered, not confirmed callable) | gap (streamable, not wired) |
|---|---|---|---|
| **NebulaStream** (this checkout) | **0 — 0.0%** | 9 — 0.5% | 1,929 — 99.5% |
| **Flink** | **1,938 — 100.0%** | 0 — 0.0% | 0 — 0.0% |
| **Kafka** | **1,938 — 100.0%** | 0 — 0.0% | 0 — 0.0% |

> L3 is **callability** — the binding invokes the function on real `libmeos`
> (correctness is inherited from MEOS's PostgreSQL suite; see the methodology's
> two-tier model). Flink/Kafka share the generated JNR-FFI facade, so their
> callability is identical. It is confirmed by a **type-aware per-method
> callability run**: each facade method is invoked in its own JVM (return /
> caught MEOS semantic error / native exit / abort = callable; only a
> linkage/marshalling exception = not-callable), with each `Pointer` argument
> built from a per-type sample inferred from the function-name tokens (the
> "connectors": `acontains_geo_tgeo` → [geometry, tgeompoint],
> `tdwithin_tgeo_tgeo` → [tgeompoint, tgeompoint]).

The headline measures callability, not facade size: the L2 facade size is not
what runs.

## NebulaStream

The checkout wires 9 streamable MEOS functions through six physical functions
under `nes-physical-operators/src/Functions/Meos/` and `MEOSWrapper.cpp`:
`aintersects_tgeo_geo`, `aintersects_tgeo_tgeo`, `econtains_geo_tgeo`,
`econtains_tgeo_geo`, `econtains_tgeo_tgeo`, `edwithin_tgeo_geo`,
`eintersects_tgeo_geo`, `eintersects_tgeo_tgeo` and `tgeo_at_stbox`. None is
confirmed callable, because the checkout carries no
`nes-systests/function/meos/` systest. The operators that
`tools/codegen/codegen_nebula.py` and `tools/codegen/codegen_aggregations.py`
emit are not committed, so the feed does not count them.

## Flink / Kafka — 100% confirmed callable, 0 residual

Every one of the 1,938 streamable functions has a facade method and is
confirmed callable on real `libmeos`. The harness builds each input from the
function-name tokens, with native `T**` arrays (`Memory.allocateDirect`) for
set/array constructors, a `trgeometryinst_make` sample for the `trgeometry`
family, `Interval`/`OffsetDateTime`/`LocalDateTime` samples for the time
functions, and an out-param buffer for the catalog/SRID out-params.

## libmeos is pinned

JMEOS loads the library via `LibraryLoader.search(<user.dir>/src).load("meos")`,
which resolves through the OS loader and **ignores `-Djnr.ffi.library.path`**.
`run_callability.sh` therefore pins the libmeos under test on
`LD_LIBRARY_PATH`; the Flink/Kafka feed is measured against the
`accumulate/parity-1.4` libmeos.

## Reason-marked (NOT streamable, never gaps)

`io-meta` 218 · `sequence-only` 14 · `ambiguous` 59 · `internal` 1,308.

## How each layer is measured

| Platform | L2 WIRED | L3 CALLABLE |
|---|---|---|
| NebulaStream | operators' `meos_call` in `*PhysicalFunction.cpp` | systest tokens (resolved to the operator by normalized-name match) whose systest is green on a local worker |
| Flink / Kafka | `public static` methods of `MeosOps*.java` facade | facade methods the type-aware per-method callability harness invoked on real libmeos without a binding failure (`callability/PerMethodCallability.java` + `run_callability.sh::run_per_method`) |

Adapters: `tools/streaming_parity/adapters/{nebula.py, jvm_facade.py}`;
callability harness: `tools/streaming_parity/callability/`. The committed feeds
and `feeds/streamable.txt` reproduce the table via `ci_gate.py` without
re-running the harness; the gate holds the Flink/Kafka floor at 1,938.

## Running-aggregation realization (Flink/Kafka vs NebulaStream)

The scope is identical on every platform: the MEOS API over a MEOS value
produced in the window (a per-group mini-trip trajectory). The MEOS operations
are the invariant C calls; only how the running aggregate's value is held
differs, driven by each platform's state model.

- **MobilityFlink / MobilityKafka** hold the windowed value as **WKB in
  checkpointed, fault-tolerant managed state** — exactly-once recovery requires
  the state to be serializable. The MEOS library is the JMEOS facade
  (`MeosOps*` methods) over a native `Pointer`; the value is reconstructed from
  WKB (`from_wkb`) for `append_tinstant` and for each function call.
- **MobilityNebula** materializes the windowed value inside a physical
  aggregate operator and applies the MEOS library to it **directly**, in
  process — no per-window WKB serialization. MEOS's expandable structures
  (`count`/`maxcount`, `appendInstant`/`appendSequence`) are the in-process
  accumulator for this model — amortized-O(1) in-place growth as each instant
  is appended. WKB appears only when a value crosses an operator boundary.

So WKB is the serialization/exchange form (mandatory in the JVM tools' state,
boundary-only in NebulaStream) and the expandable `Temporal*` is the in-memory
production form; both serve the one scope.
