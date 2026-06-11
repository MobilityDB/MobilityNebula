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
| Q5 | "pairs of vehicles meeting near P" | ◐ | ◐ | ◐ | **partial** — see below |
| Q6 | "cumulative distance per vehicle" | ✓ | ✓ | ✓ | full (via TEMPORAL_LENGTH aggregation) |
| Q7 | "first passage of each vehicle through each POI" | ✓ | ✓ | ✓ | full (per-POI fan-out) |
| Q8 | "vehicles close to a road segment (LINESTRING)" | ✓ | ✓ | ✓ | full |
| Q9 | "distance between vehicles X and Y at time T" | ◐ | ◐ | ◐ | **partial** — see below |

**27 of 27 cells** covered as scaffold YAMLs. **21 cells are full** — the BerlinMOD-Q semantic is computed entirely inside NebulaStream — and **6 cells remain partial** (Q5 × 3 forms + Q9 × 3 forms): NebulaStream emits the per-window inputs and a consumer post-processes them for the final BerlinMOD-Q answer.

### Q7 fan-out pattern (full)

NebulaStream's current SQL has no Cartesian (vehicle × POI) aggregation primitive. Q7 is therefore expressed as **one YAML per (POI, form)** — three POIs × three forms = nine YAML files. Each YAML emits the per-(window, vehicle) first-passage time for its single POI; consumers read the three POI-specific output files per form to recover the full per-(vehicle, POI) matrix. POI ids: `1` = Brussels city centre (4.3517, 50.8503, r=2 km), `2` = Anderlecht (4.3060, 50.8270, r=1 km), `3` = south of Brussels (4.2100, 50.7600, r=2 km).

### Q8 via LINESTRING (full)

MEOS' `edwithin_tgeo_geo` accepts any geometry — POINT, POLYGON, and **LINESTRING**. Q8 (vehicles within d of a road segment) is therefore expressible as a single direct predicate against a `LINESTRING(s1, s2)` geometry, no new MobilityNebula PhysicalFunction required. The segment runs from (4.30, 50.83) to (4.36, 50.87) with d = 5 km in the scaffold.

### Q6 full via TEMPORAL_LENGTH aggregation

The Q6 × 3 cells are full as of this scaffold: they use the new `TEMPORAL_LENGTH(lon, lat, ts)` aggregation, which lifts the same (lon, lat, ts) tuples as `TEMPORAL_SEQUENCE` and lowers them through a MEOS `tpoint_length(Temporal*)` call to a single `FLOAT64` result — the spheroidal length in metres of the per-(window, group) trajectory. Logical, physical, parser, and lowering wiring all live in this PR.

### Q5 / Q9 partial pattern (NebulaStream emits, consumer joins)

These two queries need a stream-self-join (Q5: pair × pair across vehicles, Q9: lookup-pair across vehicles), which is not currently a NebulaStream SQL primitive. The scaffold YAMLs express what NebulaStream can compute today:

| Q | What NebulaStream emits | What the consumer computes |
|---|---|---|
| Q5 | per-(window, vehicle) `TEMPORAL_SEQUENCE` trajectory for each near-P vehicle | per-pair distance, pair-meeting predicate, output meeting pairs |
| Q9 | per-(window, vehicle) `TEMPORAL_SEQUENCE` trajectory filtered to `vehicle_id ∈ {100, 200}` | join the two trajectories, compute the X-Y distance series |

Each partial cell IS a valid runnable NebulaStream query; the BerlinMOD-Q final answer is one consumer-side reduction step beyond the emitted output.

### Path to "full" for the two remaining partial Qs

| Q | What would make it FULL |
|---|---|
| Q5 | stream-self-join in NebulaStream SQL, OR a custom `pair_aggregate(lon, lat, ts, vehicle_id, dMeet)` Cartesian aggregation on the MobilityNebula side |
| Q9 | a custom `cross_distance_aggregate(lon, lat, ts, vehicle_id, targetA, targetB)` aggregation on the MobilityNebula side — same Cartesian shape as Q5 |

Each is a single-PR change on MobilityNebula's `nes-physical-operators` C++ surface. The patterns and templates are documented in `TemporalLengthAggregationPhysicalFunction` (this PR) and `TemporalSequenceAggregationPhysicalFunction`; the YAML schemas already exist in this scaffold so the FULL cells would be drop-in replacements once the operators land.

## MEOS operators consumed

All BerlinMOD predicates use operators already exposed by [`MobilityNebula/PR #14`](https://github.com/MobilityDB/MobilityNebula/pull/14) (and follow-up operator-add PRs):

| Operator | YAMLs using it |
|---|---|
| `edwithin_tgeo_geo(lon, lat, t, geom, d)` | Q3 × 3 forms (radius predicate, `POINT`), Q4 × 3 forms (region containment, `POLYGON` with `d=0.0`), Q8 × 3 forms (segment predicate, `LINESTRING`) |
| `TEMPORAL_SEQUENCE(lon, lat, t)` (aggregation) | Q2 × 3 forms (per-window trajectory), Q5 × 3, Q9 × 3 (partial cells) |
| `TEMPORAL_LENGTH(lon, lat, t)` (aggregation, MEOS `tpoint_length` under the hood) | Q6 × 3 forms (cumulative distance) |

`TEMPORAL_LENGTH` is added by this PR; the rest are pre-existing.

## Not covered (15 cells / 5 queries)

Marked as future work; each requires either a new MEOS operator or a NebulaStream-side extension:

| Q | Topic | Blocker |
|---|---|---|
| Q5 | pairs of vehicles meeting near P | Needs a stream-self-join across vehicles. NebulaStream's SQL needs join support OR a custom MEOS aggregation that consumes a per-vehicle map of last-known positions. |
| Q6 | cumulative distance per vehicle | Needs a custom aggregation that returns the length of the per-window `TEMPORAL_SEQUENCE` trajectory; `temporal_length(tgeo) → double` would close this. |
| Q7 | first passage of vehicles through POIs | Cartesian (vehicle × POI) state. Could be expressed as 1 query per (POI), each emitting the per-vehicle MIN(time_utc) WHERE edwithin near POI — but that's 3+ queries per snapshot form. A custom `first_passage(tgeo, geo, d) → tstzset` aggregation would close this. |
| Q8 | vehicles close to a road segment | Needs a MEOS `distance(tgeo, geometry(LINESTRING))` operator surfaced as a NebulaStream PhysicalFunction; not present in the current `Functions/Meos/` set. |
| Q9 | distance between vehicles X and Y at time T | Needs cross-vehicle pair state; same blocker as Q5 (stream-self-join or pair-aggregation). |

## Sibling parity references

- **MobilityFlink #3** — same nine queries × three forms via Flink Java code, 27/27 cells, pure-Java predicates with `TODO(meos)` markers for future JMEOS bridges.
- **MobilityKafka #1** — same nine queries × three forms via Kafka-Streams Processor API, 27/27 cells, same pure-Java predicates.
- **MobilityDB-BerlinMOD** open PRs (#29/#27/#26/#24/#23) — batch BerlinMOD-9 cross-platform reports; the snapshot form converges to those outputs as the watermark advances.

## Running

Each YAML follows the same pattern as the SNCB queries (TCP CSV source, file sink). The expected execution flow:

1. Start NebulaStream (or `MobilityNebula` docker-runtime).
2. Stream the sample CSV from `Input/input_berlinmod.csv` over TCP port `32325` (e.g. via `nc -l -p 32325 < Input/input_berlinmod.csv` or the project's existing TCP-source tooling).
3. Submit one of the YAMLs to the NebulaStream coordinator.
4. The output appears in `/workspace/Output/output_berlinmod_<q>_<form>.csv`.

YAML structure has been validated with `python3 -c "import yaml; yaml.safe_load(open(f))"` for every file. Runtime verification is gated on the NebulaStream test harness; the YAMLs are intentionally additive and the SNCB Q-series remains untouched.
