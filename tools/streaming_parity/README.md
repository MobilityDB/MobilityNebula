# Streaming-parity harness — PROVEN coverage of the MEOS surface

Streaming-platform sibling of MobilityDB's cross-type parity audit
(`tools/parity_audit/`, MobilityDB #1110). Methodology:
[`doc/methodology/streaming_parity.md`](../../doc/methodology/streaming_parity.md).

**Measures parity, does not assert it.** A MEOS function counts as covered by a
platform only when a test passes (L3) — not because it is *wirable* or merely
*registered*. Run over an **accumulated-PR build** (all open PRs merged) so the
intended surface is measured, not stale `master`.

## Layers

- **L1 EXPORTED** — symbol in the pinned `libmeos` (`nm -D`).
- **L2 WIRED** — an operator/UDF calls it (Nebula `meos_call` + SQL token;
  Flink/Kafka facade method).
- **L3 PROVEN** — a test exercising it passes (Nebula systest; Flink/Kafka JUnit).

Only L3 counts. `sequence-only` / `io-meta` / `ambiguous` / `internal` tiers are
reason-marked non-streamable — never gaps.

## Usage

```bash
# 1. on the accumulated Nebula build, run systests -> passing basenames
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
streaming-tier classification of MEOS-API's `meos-idl.json`. Flink/Kafka add an
adapter (`adapters/flink.py`, `adapters/kafka.py`) emitting the same feed shape
from `javap`/reflection (L2) × the JUnit report (L3); the core is shared.
