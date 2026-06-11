# BerlinMOD streaming forms on MobilityNebula

Additive scaffold for the **BerlinMOD-9 × 3 streaming forms** parity contract — same shape as the SQL-layer BerlinMOD-9 ([MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD)) and matching the [MobilityFlink PR #3](https://github.com/MobilityDB/MobilityFlink/pull/3) and [MobilityKafka PR #1](https://github.com/MobilityDB/MobilityKafka/pull/1) coverage on the NebulaStream runtime.

This page lives **alongside** the existing SNCB query series ([Query0..Query5](../Queries/) + [sncb_brake_monitoring](../Queries/sncb_brake_monitoring.yaml)); the SNCB Q-series and BerlinMOD-9 are sibling parity sets, not a replacement.

## Logical source

The BerlinMOD queries read from a `berlinmod_stream` logical source over TCP port `32325`, distinct from the SNCB `sncb_stream` source on port `32324`. Wire format is CSV with four columns:

```
time_utc(uint64), vehicle_id(uint64), gps_lon(float64), gps_lat(float64)
```

A sample input file is at [`Input/input_berlinmod.csv`](../Input/input_berlinmod.csv) (3 vehicles × 21 events over 14 simulated seconds).

## The three streaming forms

For each BerlinMOD reference query Q, three NebulaStream YAMLs realize the form contract:

| Form | NebulaStream pattern | Semantic |
|---|---|---|
| **continuous** | `WINDOW SLIDING(time_utc, SIZE 1 SEC, ADVANCE BY 1 SEC)` | per-event-bucket emission; consumers see a continuous stream of per-second events |
| **windowed** | `WINDOW TUMBLING(time_utc, SIZE 10 SEC)` | per-10s aggregation; one row per (window, group) |
| **snapshot** | `WINDOW TUMBLING(time_utc, SIZE 5 SEC)` | per-5s tick state; one row per (tick, group). Parity-oracle form: at each tick, the current state mirrors the batch BerlinMOD-Q result on data up to the tick |

## Coverage in this PR

| Q | Topic | Continuous | Windowed | Snapshot | Form |
|---|---|---|---|---|---|
| Q1 | "which vehicles have appeared?" | ✓ | ✓ | ✓ | full |
| Q2 | "where is vehicle X (= 200) at time T?" | ✓ | ✓ | ✓ | full |
| Q3 | "vehicles within 5 km of Brussels city centre?" | ✓ | ✓ | ✓ | full |
| Q4 | "vehicles inside Brussels-centre rectangle R?" | ✓ | ✓ | ✓ | full |
| Q5 | "pairs of vehicles meeting near P" | ✓ | ✓ | ✓ | full (via PAIR_MEETING aggregation) |
| Q6 | "cumulative distance per vehicle" | ✓ | ✓ | ✓ | full (via TEMPORAL_LENGTH aggregation) |
| Q7 | "first passage of each vehicle through each POI" | ✓ | ✓ | ✓ | full (per-POI fan-out) |
| Q8 | "vehicles close to a road segment (LINESTRING)" | ✓ | ✓ | ✓ | full |
| Q9 | "distance between vehicles X and Y at time T" | ✓ | ✓ | ✓ | full (via CROSS_DISTANCE aggregation) |

**27 of 27 cells** covered as scaffold YAMLs. **All 27 cells are full** — every BerlinMOD-Q semantic is computed entirely inside NebulaStream. The matrix is closed.

### Q7 fan-out pattern (full)

NebulaStream's current SQL has no Cartesian (vehicle × POI) aggregation primitive. Q7 is therefore expressed as **one YAML per (POI, form)** — three POIs × three forms = nine YAML files. Each YAML emits the per-(window, vehicle) first-passage time for its single POI; consumers read the three POI-specific output files per form to recover the full per-(vehicle, POI) matrix. POI ids: `1` = Brussels city centre (4.3517, 50.8503, r=2 km), `2` = Anderlecht (4.3060, 50.8270, r=1 km), `3` = south of Brussels (4.2100, 50.7600, r=2 km).

### Q8 via LINESTRING (full)

MEOS' `edwithin_tgeo_geo` accepts any geometry — POINT, POLYGON, and **LINESTRING**. Q8 (vehicles within d of a road segment) is therefore expressible as a single direct predicate against a `LINESTRING(s1, s2)` geometry, no new MobilityNebula PhysicalFunction required. The segment runs from (4.30, 50.83) to (4.36, 50.87) with d = 5 km in the scaffold.

### Q6 full via TEMPORAL_LENGTH aggregation

The Q6 × 3 cells are full as of this scaffold: they use the new `TEMPORAL_LENGTH(lon, lat, ts)` aggregation, which lifts the same (lon, lat, ts) tuples as `TEMPORAL_SEQUENCE` and lowers them through a MEOS `tpoint_length(Temporal*)` call to a single `FLOAT64` result — the spheroidal length in metres of the per-(window, group) trajectory. Logical, physical, parser, and lowering wiring all live in this PR.

### Q5 full via PAIR_MEETING aggregation

Q5 takes four input fields (lon, lat, timestamp, vehicle_id) and emits a VARSIZED string-encoded list of meeting pairs `"vid_a,vid_b,ts,<=dMeet; …"`. Upstream `edwithin_tgeo_geo` pre-filters events to the near-P set; the aggregation's `lift` step writes per-event (lon, lat, ts, vehicle_id) into a PagedVector, and the `lower` step builds a per-vehicle latest-position map, enumerates pairs in stable order, calls MEOS' `geog_dwithin` with `dMeet = 200 m` hardcoded for the scaffold, and emits pairs that meet. Future PR can parameterize `dMeet` via a constant input.

### Q9 full via CROSS_DISTANCE aggregation

Q9 takes the same four input fields and emits a FLOAT64 — the spheroidal distance between the two target vehicles (VID_A = 100, VID_B = 200 hardcoded) at their latest known positions in the window. NaN when either is unobserved. Implemented via the MEOS `nad_tgeo_tgeo` path over single-instant tgeompoints. Future PR can parameterize (VID_A, VID_B).

## MEOS operators consumed

All BerlinMOD predicates use operators already exposed by [`MobilityNebula/PR #14`](https://github.com/MobilityDB/MobilityNebula/pull/14) (and follow-up operator-add PRs):

| Operator | YAMLs using it |
|---|---|
| `edwithin_tgeo_geo(lon, lat, t, geom, d)` | Q3 × 3 forms (radius predicate, `POINT`), Q4 × 3 forms (region containment, `POLYGON` with `d=0.0`), Q5 × 3 forms (upstream near-P filter), Q8 × 3 forms (segment predicate, `LINESTRING`) |
| `TEMPORAL_SEQUENCE(lon, lat, t)` (aggregation) | Q2 × 3 forms (per-window trajectory) |
| `TEMPORAL_LENGTH(lon, lat, t)` (aggregation, MEOS `tpoint_length` under the hood) | Q6 × 3 forms (cumulative distance) |
| `PAIR_MEETING(lon, lat, t, vehicle_id)` (aggregation, MEOS `geog_dwithin` per pair under the hood) | Q5 × 3 forms (meeting pairs) |
| `CROSS_DISTANCE(lon, lat, t, vehicle_id)` (aggregation, MEOS `nad_tgeo_tgeo` under the hood) | Q9 × 3 forms (cross-vehicle distance) |

`PAIR_MEETING` and `CROSS_DISTANCE` are added by this PR (and `TEMPORAL_LENGTH` is added by the parent #16); the rest are pre-existing.

## Streaming-semantics tier overlay

Each BerlinMOD-Q in this scaffold falls into one of the four streaming-execution tiers used by the per-binding wirings work across the ecosystem. The vocabulary is the closed 7-value set proposed for the MEOS-API catalog as `objectModel.streamingSemantics` (see the MEOS-API #10 sibling-facet RFC).

The mapping makes the cross-binding picture explicit — a Q's tier on NebulaStream is the same tier it would land in on Flink / Kafka. The right-most column points to the equivalent generic wiring on Flink (where adopters consume the v4 baseline through generic DataStream wrappers).

| Tier | BerlinMOD-Q | NebulaStream realization | Equivalent Flink wiring |
|---|---|---|---|
| `stateless` | Q1 (distinct-vehicle observation) | Simple SQL aggregation; no MEOS handle | `MeosStatelessMap` / `MeosStatelessFilter` |
| `bounded-state` | Q2, Q3, Q4, Q7 (per-vehicle / per-POI predicate state), Q8 | Aggregations that hold per-key latest position (TEMPORAL_SEQUENCE pattern); single MEOS-temporal evaluation | `MeosBoundedStateMap` (per-key `ValueState<byte[]>`) |
| `windowed` | Q6 (per-window trajectory length) | Custom MEOS aggregation closing the window once and emitting a scalar (`TEMPORAL_LENGTH`) | `MeosWindowedAggregate` (window-close-only) |
| `cross-stream` | Q5 (pair meeting), Q9 (cross-vehicle distance) | Four-field aggregations holding per-(vehicle-pair) state inside one operator (`PAIR_MEETING`, `CROSS_DISTANCE`) — same row-set sees all vehicles, so the "stream-self-join" is a single-aggregation enumeration rather than two streams | `MeosCrossStreamJoin` (`KeyedStream.intervalJoin`) |
| `io-meta` / `sequence-only` | — | not exercised by the BerlinMOD-9 set | n/a |

### Why the cross-stream tier looks different on NebulaStream

On Flink, the cross-stream tier maps to `KeyedStream.intervalJoin(other)` — two distinct keyed streams paired within a time bound. On NebulaStream, the same semantic is realized inside a single windowed aggregation that holds per-(vehicle-pair) state and enumerates pairs at window close. The two are equivalent: both materialize the Cartesian-product evaluation, just at different points in the operator topology. The tier classification is on the **MEOS semantic**, not on the engine pattern — and Q5 / Q9 are unambiguously `cross-stream` regardless of which engine realizes them.

### Why Q7 is bounded-state, not windowed

Q7 ("first passage of each vehicle through each POI") would naturally read as windowed (per-window minimum). It's classified bounded-state here because the NebulaStream scaffold expresses it as a per-POI fan-out (one YAML per POI), each YAML computing the per-vehicle latest-known position predicate and selecting the per-(vehicle, POI) earliest qualifying timestamp inside the window. The state per (vehicle, POI) is bounded; no per-window reduction across the full sequence is needed.

## Sibling parity references

- **MobilityFlink** — same nine queries × three forms on Flink. Original scaffold landed; the per-tier wiring infrastructure that mechanically wraps any of the 2,097 generated MEOS facade methods into Flink DataStream operators lives in the [`org.mobilitydb.flink.meos.wirings`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/org/mobilitydb/flink/meos/wirings) package (5 generic classes covering 100% of the streamable + io-meta surface; a capstone demo composes all four tiers into one pipeline).
- **MobilityKafka** — same nine queries × three forms on Kafka Streams, with a codegen mirror of the MEOS facade in [`org.mobilitydb.kafka.meos`](https://github.com/MobilityDB/MobilityKafka/blob/main/kafka-streams-app/src/main/java/org/mobilitydb/kafka/meos).
- **MobilityDB-BerlinMOD** — batch BerlinMOD-9 cross-platform reports; the snapshot form on the streaming side converges to those outputs as the watermark advances.

## Running

Each YAML follows the same pattern as the SNCB queries (TCP CSV source, file sink). The expected execution flow:

1. Start NebulaStream (or `MobilityNebula` docker-runtime).
2. Stream the sample CSV from `Input/input_berlinmod.csv` over TCP port `32325` (e.g. via `nc -l -p 32325 < Input/input_berlinmod.csv` or the project's existing TCP-source tooling).
3. Submit one of the YAMLs to the NebulaStream coordinator.
4. The output appears in `/workspace/Output/output_berlinmod_<q>_<form>.csv`.

YAML structure has been validated with `python3 -c "import yaml; yaml.safe_load(open(f))"` for every file. Runtime verification is gated on the NebulaStream test harness; the YAMLs are intentionally additive and the SNCB Q-series remains untouched.
