# BerlinMOD streaming forms on MobilityNebula

The nine BerlinMOD queries ([MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD)), each in three streaming forms, as NebulaStream query YAMLs under [`Queries/berlinmod/`](../Queries/berlinmod/). The same nine queries in the same three forms run on Flink ([MobilityFlink](https://github.com/MobilityDB/MobilityFlink)) and Kafka Streams ([MobilityKafka](https://github.com/MobilityDB/MobilityKafka)).

They sit beside the SNCB query series ([Query0..Query5](../Queries/) and [sncb_brake_monitoring](../Queries/sncb_brake_monitoring.yaml)); the two are separate query sets.

## Logical source

The BerlinMOD queries read the `berlinmod_stream` logical source over TCP port `32325`, distinct from the SNCB `sncb_stream` source on port `32324`. The wire format is CSV with four columns:

```
time_utc(uint64), vehicle_id(uint64), gps_lon(float64), gps_lat(float64)
```

A sample input is [`Input/input_berlinmod.csv`](../Input/input_berlinmod.csv): 3 vehicles, 21 events over 14 seconds, each vehicle at one fixed position.

## The three streaming forms

| Form | NebulaStream pattern | Semantics |
|---|---|---|
| **continuous** | `WINDOW SLIDING(time_utc, SIZE 1 SEC, ADVANCE BY 1 SEC)` | one result per one-second bucket |
| **windowed** | `WINDOW TUMBLING(time_utc, SIZE 10 SEC)` | one row per (10-second window, group) |
| **snapshot** | `WINDOW TUMBLING(time_utc, SIZE 5 SEC)` | one row per (5-second tick, group); at each tick the state answers the batch BerlinMOD query over the data up to the tick |

## The nine queries

Each query has three YAMLs, `q<N>_continuous.yaml`, `q<N>_windowed.yaml` and `q<N>_snapshot.yaml`; Q7 has three per POI. Every query is computed entirely inside NebulaStream.

| Q | Question | Realization |
|---|---|---|
| Q1 | which vehicles have appeared? | `COUNT` grouped by vehicle |
| Q2 | where is vehicle 200? | `TEMPORAL_SEQUENCE(lon, lat, t)` filtered to vehicle 200 |
| Q3 | which vehicles are near Brussels city centre? | `edwithin_tgeo_geo` against `POINT(4.3517 50.8503)` with distance argument `5000.0` |
| Q4 | which vehicles are inside the Brussels-centre rectangle? | `edwithin_tgeo_geo` against a `POLYGON` with distance argument `0.0` |
| Q5 | which pairs of vehicles meet near P? | `edwithin_tgeo_geo` pre-filter, then `PAIR_MEETING(lon, lat, t, vehicle_id, 200.0)` |
| Q6 | how far does each vehicle travel? | `TEMPORAL_LENGTH(lon, lat, t)` grouped by vehicle |
| Q7 | when does each vehicle first pass each POI? | one YAML per (POI, form), `MIN(time_utc)` under `edwithin_tgeo_geo` |
| Q8 | which vehicles are near a road segment? | `edwithin_tgeo_geo` against `LINESTRING(4.30 50.83, 4.36 50.87)` with distance argument `5000.0` |
| Q9 | how far apart are vehicles 100 and 200? | `CROSS_DISTANCE(lon, lat, t, vehicle_id, 100, 200)` |

### Q7: one YAML per POI

NebulaStream SQL has no Cartesian (vehicle × POI) aggregation, so Q7 is one YAML per (POI, form): three POIs by three forms, nine files. Each YAML emits, per (window, vehicle), the first timestamp at which the vehicle is near its POI; the three POI outputs of one form together give the per-(vehicle, POI) first-passage matrix. The POIs are `1` = Brussels city centre (4.3517, 50.8503, distance argument 2000.0), `2` = Anderlecht (4.3060, 50.8270, distance argument 1000.0) and `3` = south of Brussels (4.2100, 50.7600, distance argument 2000.0).

### Q8: a LINESTRING operand

`edwithin_tgeo_geo` accepts any geometry operand (POINT, POLYGON or LINESTRING), so Q8 is one predicate against the road segment.

## The three BerlinMOD aggregations

Q5, Q6 and Q9 need every event of a window at once, so each is a windowed aggregation. All three take positions as WGS84 longitude and latitude and measure on the spheroid in metres; the latest position of a vehicle in a window is the event with the greatest timestamp, whatever the arrival order.

| Aggregation | Arguments | Result |
|---|---|---|
| `TEMPORAL_LENGTH` | lon, lat, ts | FLOAT64: MEOS `tpoint_length(seq, true)` of the linear tgeogpoint sequence through the window's events in time order. One instant has length 0; two different positions at one timestamp yield NaN. |
| `PAIR_MEETING` | lon, lat, ts, vehicle_id, dMeet | VARSIZED: `"vidA,vidB,ts,<=dMeet;..."`, every vehicle pair (vidA < vidB) whose latest positions satisfy MEOS `geog_dwithin(a, b, dMeet, true)`, stamped with the later of the two timestamps. |
| `CROSS_DISTANCE` | lon, lat, ts, vehicle_id, vidA, vidB | FLOAT64: MEOS `geog_distance(a, b, true)` between the latest positions of vidA and vidB; NaN when either has no event in the window. |

dMeet, vidA and vidB are numeric constants in the query. The serialized plan carries them as literals, so the worker builds each aggregation with the values the query states.

## Streaming-semantics tiers

Each query falls into one of the streaming-execution tiers of the `objectModel.streamingSemantics` vocabulary. A query's tier is a property of its MEOS semantics, so it is the same on NebulaStream, Flink and Kafka.

| Tier | BerlinMOD-Q | NebulaStream realization |
|---|---|---|
| `stateless` | Q1 | plain SQL aggregation, no MEOS value |
| `bounded-state` | Q2, Q3, Q4, Q7, Q8 | per-key predicate state or a per-window trajectory, one MEOS evaluation per event |
| `windowed` | Q6 | a MEOS aggregation that closes the window once and emits a scalar (`TEMPORAL_LENGTH`) |
| `cross-stream` | Q5, Q9 | a four-field aggregation that sees every vehicle of the window and enumerates pairs inside one operator (`PAIR_MEETING`, `CROSS_DISTANCE`) |

On Flink the cross-stream tier is `KeyedStream.intervalJoin` over two keyed streams; on NebulaStream one windowed aggregation holds the state of every vehicle and enumerates the pairs at window close. Both evaluate the same pairs.

Q7 is bounded-state rather than windowed: each per-POI YAML keeps, per (vehicle, POI), the earliest qualifying timestamp of the window, which is bounded state with no reduction over a full sequence.

## Running

Each YAML follows the SNCB queries' pattern (TCP CSV source, file sink):

1. Start NebulaStream (or the MobilityNebula docker runtime).
2. Stream `Input/input_berlinmod.csv` over TCP port `32325`, e.g. `nc -l -p 32325 < Input/input_berlinmod.csv`.
3. Submit one YAML to the NebulaStream coordinator.
4. Read the output at `/workspace/Output/output_berlinmod_<q>_<form>.csv`.
