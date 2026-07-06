# Streaming-parity methodology — PROVEN (measured, not guessed)

This is the streaming-platform sibling of MobilityDB's cross-type parity
methodology (`doc/methodology/cross_type_parity.md`, MobilityDB #1002) and its
audit harness (`tools/parity_audit/`, MobilityDB #1110). It answers one
question for each streaming platform (NebulaStream · Flink · Kafka):

> Of the MEOS functions that *can* run in a per-event / windowed stream, how
> many does this platform actually support — **proven by a passing test**?

## The problem it fixes

Streaming-side parity was previously reported as a **guessed** number — e.g.
"2,097 / 2,097 MEOS functions wirable via 5 generic classes", "27/27 cells
full". Those counts measure *wirability* (a generic class *could* wrap the
function) or *registration* (an operator exists), **not** that the function
ever ran correctly. When the operators were finally executed, the gap was
stark: the systests had never run at all (wrong format + wrong location), and
running them surfaced three latent defects. **Wirable ≠ wired ≠ working.**

## The three-layer backing gate

A MEOS function counts as *covered* by a platform only at the deepest layer it
reaches; **only L3 counts toward TRUE parity**:

| Layer | Meaning | How it is introspected |
|---|---|---|
| **L1 EXPORTED** | the symbol exists in the pinned `libmeos` | `nm -D --defined-only libmeos.so` |
| **L2 WIRED** | the platform registers an operator/UDF that calls it | Nebula: the operator's `meos_call` + the SQL parser token; Flink/Kafka: a generated facade method (`javap`/reflection) |
| **L3 CALLABLE** | the binding can actually **invoke** it on real `libmeos` (valid input → no linkage / marshalling / binding failure) | Nebula: a systest exercises the operator; Flink/Kafka: a **callability test** invokes the facade method |

"Registered" is not "covered" and "wirable" is not "covered" — exactly as, in
the DB methodology, *symbol-exported ≠ backing* and *registration ≠ backing*.

## Two tiers: callability (per platform) + correctness (inherited from MEOS)

L3 is deliberately **callability**, not result-correctness. There are two
distinct test purposes, mirroring how MEOS itself is tested:

- **Callability (per platform, what L3 measures).** Can this platform's binding
  *reach* the MEOS function — symbol resolves, arguments marshal in, the result
  marshals back, no linkage/marshalling error? This is the streaming sibling of
  MEOS's own *callability* tests ("the connection for the function is done").
  It is per-platform because each binding (Nebula codegen; the JNR-FFI facade
  shared by Flink & Kafka) is an independent connection that can be wrong on its
  own. A MEOS *semantic* error on a synthetic input still proves callability —
  the call reached MEOS; only a binding/linkage/marshalling failure means
  not-callable.
- **Correctness (inherited, NOT re-tested per platform).** Does the function
  return the right answer? This is verified **once, upstream**, by
  MEOS/MobilityDB's PostgreSQL regression suite — the SQL executes the *same*
  MEOS C code the bindings call. Re-checking it on every stream platform would
  be redundant and, at 1,949 functions × 3 platforms, intractable.

So the streaming-parity question is precisely **"is every streamable MEOS
function callable through this platform?"** — correctness rides along from MEOS.

## The instrument: accumulated-PR builds

Parity is measured against an **accumulated-PR build** of each platform — all
the platform's open PRs merged into one integration build — not stale `master`.
The shortfall is overwhelmingly *un-accumulated open PRs*, not unimplemented
work, so measuring stale `master` understates the true intended surface. The
harness is run over the accumulated build.

- **NebulaStream**: the `feat/nebula-codegen-w21-tcbuffer-nad` tip (stacks
  #21→#42) accumulates every codegen wave + its systests; built once
  (`build-w15` in the `mobilitynebula-v3` dev image, which bakes the ways CSV).
- **Flink**: accumulate #3/#4/#5 (BerlinMOD + tier-aware facade codegen) and
  #6→#10 (wirings) into one Maven build.
- **Kafka**: accumulate #1/#2/#3/#4 into one Gradle/Maven build.

## The streamable surface + reason-marked non-streamable tiers

The reference "expected" surface is the **streamable** MEOS public API, taken
from the streaming-relevance baseline (the v4 classifier over MEOS-API's
`meos-idl.json`): tiers `stateless`, `bounded-state`, `windowed`,
`cross-stream` = **1,949** functions.

The remaining tiers are **reason-marked exclusions** — the streaming sibling of
the *semantic / structural* exclusions in `cross_type_parity.md`. They are
**never gaps** and must never be "implemented to close":

| Tier | Count | Reason |
|---|---|---|
| `io-meta` | 218 | I/O / catalog / infrastructure — not a streaming domain operator |
| `sequence-only` | 14 | needs a whole completed sequence, not a per-event handle; only reachable as a windowed closure-of-stream |
| `ambiguous` | 59 | open streaming-semantics design question (the formal `streamingSemantics` facet RFC) |
| `internal` | 1,308 | not part of the public MEOS API |

## The harness

`tools/streaming_parity/` (config-driven, mirroring `tools/parity_audit/`):

- `streaming_parity.py` — platform-agnostic core. Consumes the streamable
  catalog (JSON) + a per-platform **feed** (`function<TAB>{proven|wired}`) and
  prints: the L3/L2/gap table, the reason-marked non-streamable section, the
  honest *wired-but-unproven backlog*, and the real gaps per tier.
- `adapters/nebula.py` — introspects the accumulated Nebula build:
  operators' `meos_call`s (L2) and the systest tokens whose tests pass (L3,
  resolved authoritatively through the parser dispatch).
- `adapters/jvm_facade.py` — the Flink/Kafka adapter: `public static` facade
  method names (L2) cross-referenced with the confirmed-callable set (L3) from
  the `callability/` harness over real `libmeos`.

```bash
# Nebula, on the accumulated build:
#   run the systests, capture the passing basenames, build the feed, measure.
adapters/nebula.py --root . --passing passing.txt > nebula.feed.tsv
streaming_parity.py --catalog streaming-relevance-baseline.json \
                    --platform NebulaStream --feed nebula.feed.tsv
```

## Measured baseline (NebulaStream, 2026-05-22)

Against the accumulated build, with all 28 systests passing:

| layer | count | % of 1,949 streamable |
|---|---|---|
| **L3 PROVEN** (tested green) | **8** | **0.4%** |
| L2 wired-only (registered, untested) | 77 | 4.0% |
| gap (streamable, not wired) | 1,864 | 95.6% |

This is the honest replacement for the former "100% wirable" headline: the
most-developed streaming platform has **8 proven** MEOS functions, not 2,097.
The 77 wired-but-unproven are the immediate backlog (add one test each); the
1,864 gaps are the real codegen frontier.

The cross-platform run is in
[`streaming_parity_assessment.md`](./streaming_parity_assessment.md):
**Nebula 8 / Flink 1,472 / Kafka 1,472** callable of 1,949 streamable.

### Callability harness (Flink/Kafka) + the type-aware "connectors"

`tools/streaming_parity/callability/` is a reflection prover for the shared
JNR-FFI `MeosOps*.java` facade. `PerMethodCallability.java` invokes ONE
(class, method) per JVM so a type-mismatch `SIGSEGV` or a native `exit(1)` on a
MEOS semantic error only loses the method under test; the driver
(`run_callability.sh::run_per_method`) classifies *reached native MEOS* (clean
return / caught MEOS error / native exit / signal) as **callable** and only a
linkage/marshalling exception (`BINDFAIL`) as not-callable. The **connectors**:
each `Pointer` arg is built from a per-type sample inferred from the
function-name tokens (`acontains_geo_tgeo` → [geometry, tgeompoint]) via a
`token → (*_in constructor, literal)` table — this is what lifted measured
callability from a single-primary-value floor (331) to **1,472** (the
multi-`Pointer` spatial-relations and span/set/box operators).

Crucially, the residual is **`nm -D`-attributed, not guessed**: 302 are
extended-type ops whose constructor is *absent* from the linked libmeos
(cbuffer/pose/tcbuffer/tpose/trgeo — gated on the extended-type C-API #1081–1085,
a reason-marked gap, not a binding defect), ~173 are present-in-libmeos but take
arrays / aggregate-state / type-enum args the literal-synth can't build
(correctness inherited from the MEOS PostgreSQL suite), and 3 are
declared-not-built defects (`tfloat_avg_value`, `tnumber_trend`,
`geog_from_binary`). The `tfloat_avg_value` defect was found *independently* on
the JVM facade and in Nebula's W8 — a cross-platform validation.

## Reaching 100% PROVEN

1. **Per-function tests, generated.** Extend each platform's codegen so every
   emitted operator ships a systest/JUnit that exercises it — turning L2→L3 in
   bulk. (Nebula systests use the DDL format documented in the systest
   reference; the generator already emits one per shape — extend to one per
   operator.)
2. **Drive the real gaps** by tier, reusing the cross-type reason-marked
   exclusions (a function meaningless/structurally-impossible for a type is
   equally not-a-gap on a stream).
3. **CI parity gate.** Wire `streaming_parity.py` into CI over the accumulated
   build; fail the build if L3-proven regresses or if a "100%" claim is made
   while the gap list is non-empty. This makes a false 100% impossible by
   construction — the same gate the DB methodology adds for MobilityDB.

Non-negotiable, exactly as in the DB methodology: **measure parity, do not
assert it.** A function is covered only when a test passes.
