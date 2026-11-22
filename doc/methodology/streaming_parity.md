# Streaming-parity methodology — PROVEN (measured, not guessed)

This is the streaming-platform sibling of MobilityDB's cross-type parity
methodology and its audit harness. It answers one question for each streaming
platform (NebulaStream · Flink · Kafka):

> Of the MEOS functions that *can* run in a per-event / windowed stream, how
> many does this platform actually support — **proven by a passing test**?

A count of *wirable* functions (a generic class could wrap the function) or of
*registered* operators (an operator exists) says nothing about whether the
function ever runs. **Wirable ≠ wired ≠ working**, so only an executed test
counts.

## The three-layer backing gate

A MEOS function counts as *covered* by a platform only at the deepest layer it
reaches; **only L3 counts toward parity**:

| Layer | Meaning | How it is introspected |
|---|---|---|
| **L1 EXPORTED** | the symbol exists in the pinned `libmeos` | `nm -D --defined-only libmeos.so` |
| **L2 WIRED** | the platform registers an operator/UDF that calls it | Nebula: the operator's `meos_call` + the SQL parser token; Flink/Kafka: a generated facade method (`javap`/reflection) |
| **L3 CALLABLE** | the binding invokes it on real `libmeos` (valid input → no linkage / marshalling / binding failure) | Nebula: a systest exercises the operator; Flink/Kafka: a **callability test** invokes the facade method |

"Registered" is not "covered" and "wirable" is not "covered" — exactly as, in
the DB methodology, *symbol-exported ≠ backing* and *registration ≠ backing*.

## Two tiers: callability (per platform) + correctness (inherited from MEOS)

L3 is **callability**, not result-correctness. There are two distinct test
purposes, mirroring how MEOS itself is tested:

- **Callability (per platform, what L3 measures).** Can this platform's binding
  *reach* the MEOS function — symbol resolves, arguments marshal in, the result
  marshals back, no linkage/marshalling error? It is per-platform because each
  binding (Nebula codegen; the JNR-FFI facade shared by Flink & Kafka) is an
  independent connection that can be wrong on its own. A MEOS *semantic* error
  on a synthetic input still proves callability — the call reached MEOS; only a
  binding/linkage/marshalling failure means not-callable.
- **Correctness (inherited, NOT re-tested per platform).** Does the function
  return the right answer? MEOS/MobilityDB's PostgreSQL regression suite answers
  it once, upstream — the SQL executes the *same* MEOS C code the bindings call.

So the streaming-parity question is precisely **"is every streamable MEOS
function callable through this platform?"** — correctness rides along from MEOS.

## The instrument: one feed per platform build

A feed (`function<TAB>{proven|wired}`) describes one build of one platform, and
the measurement is of that build. `adapters/nebula.py` reads a Nebula checkout
plus the list of its green systests; `adapters/jvm_facade.py` reads the
generated Flink/Kafka facade plus the list of methods the callability prover
invoked.

## The streamable surface + reason-marked non-streamable tiers

The reference surface is the **streamable** MEOS public API, taken from the
streaming-relevance baseline (the v4 classifier over MEOS-API's
`meos-idl.json`): tiers `stateless`, `bounded-state`, `windowed`,
`cross-stream` = **1,938** functions (`tools/streaming_parity/feeds/streamable.txt`).

The remaining tiers are **reason-marked exclusions** — the streaming sibling of
the *semantic / structural* exclusions of the cross-type methodology. They are
**never gaps** and are never "implemented to close":

| Tier | Count | Reason |
|---|---|---|
| `io-meta` | 218 | I/O / catalog / infrastructure — not a streaming domain operator |
| `sequence-only` | 14 | needs a whole completed sequence, not a per-event handle; only reachable as a windowed closure-of-stream |
| `ambiguous` | 59 | open streaming-semantics design question (the formal `streamingSemantics` facet RFC) |
| `internal` | 1,308 | not part of the public MEOS API |

## The harness

`tools/streaming_parity/`:

- `streaming_parity.py` — platform-agnostic core. Consumes the streamable
  catalog (JSON) + a per-platform feed and prints the L3/L2/gap table, the
  reason-marked non-streamable section, the *wired-but-unproven backlog* and the
  real gaps per tier.
- `adapters/nebula.py` — Nebula: operators' `meos_call`s (L2) and the systest
  tokens of the green systests (L3, resolved through the parser dispatch).
- `adapters/jvm_facade.py` — Flink/Kafka: `public static` facade method names
  (L2) cross-referenced with the confirmed-callable set (L3) from `callability/`.
- `ci_gate.py` — the CI gate (`.github/workflows/streaming_parity_gate.yml`):
  fails when confirmed-callable drops below the floor, when the assessment
  states a percentage above the measured one while a gap remains, or when the
  assessment does not state the measured count.

```bash
adapters/nebula.py --root . --passing passing.txt > nebula.feed.tsv
streaming_parity.py --catalog streaming-relevance-baseline.json \
                    --platform NebulaStream --feed nebula.feed.tsv
```

### Callability harness (Flink/Kafka) + the type-aware "connectors"

`tools/streaming_parity/callability/` is a reflection prover for the shared
JNR-FFI `MeosOps*.java` facade. `PerMethodCallability.java` invokes ONE
(class, method) per JVM, so a type-mismatch `SIGSEGV` or a native `exit(1)` on a
MEOS semantic error only loses the method under test; the driver
(`run_callability.sh::run_per_method`) classifies *reached native MEOS* (clean
return / caught MEOS error / native exit / signal) as **callable** and only a
linkage/marshalling exception (`BINDFAIL`) as not-callable. The **connectors**:
each `Pointer` argument is built from a per-type sample inferred from the
function-name tokens (`acontains_geo_tgeo` → [geometry, tgeompoint]) via a
`token → (*_in constructor, literal)` table. The residual is attributed by
`nm -D` and facade introspection, never estimated.

## Reaching 100% PROVEN

1. **Per-function tests, generated.** Each platform's codegen ships a
   systest/JUnit per emitted operator, turning L2 into L3 in bulk.
2. **Drive the real gaps** by tier, reusing the cross-type reason-marked
   exclusions (a function meaningless or structurally impossible for a type is
   equally not-a-gap on a stream).
3. **CI parity gate.** `ci_gate.py` fails the build if L3 regresses or if a
   "100%" claim is made while the gap list is non-empty, so a false 100% is
   impossible by construction.

**Measure parity, do not assert it.** A function is covered only when a test
invokes it.
