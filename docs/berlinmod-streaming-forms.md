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
