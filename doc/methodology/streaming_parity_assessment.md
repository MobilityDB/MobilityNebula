# Streaming-parity assessment — the 3 platforms, measured

Result of the [streaming-parity methodology](./streaming_parity.md) across the
three streaming platforms, each over its accumulated-PR build. The reference
surface is the **1,945** streamable MEOS public functions (tiers
`stateless`/`bounded-state`/`windowed`/`cross-stream`).

| Platform | **L3 CALLABLE** (binding invokes it, confirmed) | L2 wired-only (registered, not yet confirmed callable) | gap (streamable, not wired) |
|---|---|---|---|
| **NebulaStream** | **6 — 0.3%** | 237 — 12.2% | 1,702 — 87.5% |
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
out-params. The 4 functions the MEOS API cleanup removed (`tfloat_avg_value`,
`geog_from_binary`, `srid_check_latlong`, `nad_stbox_trgeometry`) are no longer
in the surface (1,949 → 1,945).

## Reason-marked (NOT streamable, never gaps)

`io-meta` 218 · `sequence-only` 14 · `ambiguous` 59 · `internal` 1,308.

## How each layer is measured

| Platform | L2 WIRED | L3 CALLABLE |
|---|---|---|
| NebulaStream | operators' `meos_call` in `*PhysicalFunction.cpp` | systest tokens (resolved via the parser dispatch) whose test passed |
| Flink / Kafka | `public static` methods of `MeosOps*.java` facade | facade methods the type-aware per-method callability harness invoked on real libmeos without a binding failure (`callability/PerMethodCallability.java` + `run_callability.sh::run_per_method`) |

Adapters: `tools/streaming_parity/adapters/{nebula.py, jvm_facade.py}`;
callability harness: `tools/streaming_parity/callability/`. The committed
`feeds/flink-kafka.feed.tsv` + `feeds/streamable.txt` reproduce the table via
`ci_gate.py` without re-running the harness.

## Status

- **Flink / Kafka: 100% PROVEN** (1,945 / 1,945 confirmed callable on real
  libmeos). The CI gate (`ci_gate.py` + `.github/workflows/streaming_parity_gate.yml`)
  holds the floor at 1,945 and blocks any regression or over-claim; the committed
  feed reproduces it without re-running the harness.
- **NebulaStream: in progress.** 243 / 1,945 wired (operators emitted + parser-glued),
  6 confirmed callable via runnable systests. Every wired operator is **locally
  compile-verified**: the generated `nes-{physical,logical}-operators` +
  `nes-sql-parser` libraries link clean in the `nebulastream/nes-development`
  dev image against the accumulate-PR `libmeos` (the build under test). Wave 22
  added the 60-operator comparison family (`ever_`/`always_` scalar + two-temporal,
  tfloat/tint) via the descriptor-builder (`tools/codegen/build_descriptor.py`),
  which classifies gap functions by their exact in-header signature so emission
  stays measured-not-guessed; Wave 23 added 18 spatial-relation + comparison
  operators (`*_tgeo_geo` / `*_tgeo_tgeo` / `*_tcbuffer_*`) reusing existing
  templates. The remaining path lifts more families
  (typed comparison, arithmetic, accessors, transforms) by the same generate →
  compile-check loop, and adds a systest per operator to raise the confirmed-callable
  count toward the JVM tools' 100%.
