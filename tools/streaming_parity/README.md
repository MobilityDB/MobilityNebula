# Streaming-parity harness — PROVEN coverage of the MEOS surface

Streaming-platform sibling of MobilityDB's cross-type parity audit. Methodology:
[`doc/methodology/streaming_parity.md`](../../doc/methodology/streaming_parity.md);
measured result: [`doc/methodology/streaming_parity_assessment.md`](../../doc/methodology/streaming_parity_assessment.md).

**Measures parity, does not assert it.** A MEOS function counts as covered by a
platform only when a test invokes it (L3) — not because it is *wirable* or
merely *registered*. A feed describes one build of a platform, and the
measurement is of that build.

## Layers

- **L1 EXPORTED** — symbol in the pinned `libmeos` (`nm -D`).
- **L2 WIRED** — an operator/UDF calls it (Nebula `meos_call` + SQL token;
  Flink/Kafka facade method).
- **L3 CALLABLE** — a green test invokes it (Nebula systest; Flink/Kafka
  per-method callability run).

Only L3 counts. `sequence-only` / `io-meta` / `ambiguous` / `internal` tiers are
reason-marked non-streamable — never gaps.

## Files

- `streaming_parity.py` — platform-agnostic core: streamable catalog + feed → the L3/L2/gap table.
- `ci_gate.py` — the CI gate over the committed feeds (no regression, no over-claim, doc consistent).
- `adapters/nebula.py` — Nebula feed from a checkout (+ the passing-systest list).
- `adapters/jvm_facade.py` — Flink/Kafka feed from the generated `MeosOps*.java` facade (+ the callable list).
- `callability/` — the per-method JVM callability prover for the Flink/Kafka facade.
- `feeds/` — the committed feeds and the streamable list (see [`feeds/README.md`](feeds/README.md)).

## Usage

```bash
# 1. on a Nebula build: the basenames of the green MEOS systests
for t in nes-systests/function/meos/*.test; do
  build-<dir>/nes-systests/systest/systest -t "$t" 2>&1 | grep -q "All queries passed" \
    && basename "$t" .test
done > passing.txt

# 2. adapter -> coverage feed (function<TAB>{proven|wired})
python3 tools/streaming_parity/adapters/nebula.py --root . --passing passing.txt > nebula.feed.tsv

# 3. measure against the streamable catalog
python3 tools/streaming_parity/streaming_parity.py \
  --catalog streaming-relevance-baseline.json \
  --platform NebulaStream --feed nebula.feed.tsv
```

The streamable catalog (`streaming-relevance-baseline.json`) is the v4
streaming-tier classification of MEOS-API's `meos-idl.json`;
`feeds/streamable.txt` holds its 1,938 streamable function names.
