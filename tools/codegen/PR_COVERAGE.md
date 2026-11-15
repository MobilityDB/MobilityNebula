# Where each open pull request stands against the generator

Every operator a pull request carries is matched against the set the
generator emits from the MEOS-API catalog. A pull request is SUPERSEDED when
the generator emits every operator it carries, PARTIAL when it emits some,
and UNCOVERED when it emits none of them.

The operator lists come from each branch's full diff against main, so a
branch stacked on another reports its whole chain rather than its own
addition. That widens a count, never narrows it: a pull request reported
SUPERSEDED is superseded together with everything beneath it.

Across the 88 open pull requests there are 438 distinct operators, of which the
generator emits 174. The generator emits 463 operators in all, so 289 of them
are carried by no open pull request: the two sets overlap in under half of
either, and the generator is the larger.

## Superseded — the generator emits every operator these carry

| PR | operators | title |
|---|---|---|
| #45 | 59/59 | feat(nebula): Add the ever/always comparison family and signature-driven descriptor builder |
| #46 | 59/59 | feat(nebula): Add 18 int-returning spatial-relation and comparison operators via existing templates |
| #47 | 59/59 | feat(nebula): Add the generalized per-event assembler and scalar operator families |
| #48 | 59/59 | tools(nebula): Add build_local.sh dev-image build script with auto MQTT toggle |
| #49 | 59/59 | feat(nebula): Add the extract marshaler for unary Temporal-to-scalar transforms |
| #50 | 59/59 | feat(nebula): Add the box/span query-literal family via per-event box-literal assembler |
| #51 | 59/59 | feat(nebula): Add windowed extent-to-box aggregates with VARSIZED output |
| #52 | 59/59 | feat(nebula): Add value/time Span extent aggregates with scalar fold |
| #53 | 59/59 | feat(nebula): Add 55 box/temporal position predicates in both argument orders |
| #54 | 59/59 | feat(nebula): Add windowed value-union set-collect aggregates |
| #55 | 59/59 | feat(nebula): Add durable query-literal round-trip for parameterized aggregates |
| #56 | 59/59 | feat(nebula): Add MEOS function library composition over VARSIZED hex-WKB values |
| #57 | 59/59 | feat(nebula): Add TRAJECTORY_WKB windowed aggregate emitting the mini-trip as hex-WKB |
| #58 | 59/59 | feat(nebula): Add the expandable-Temporal* in-process streaming aggregate substrate |
| #59 | 59/59 | feat(nebula): Add value-output windowed aggregates emitting Temporal*-transform results as hex-WKB |
| #60 | 59/59 | feat(nebula): Add tnumber value-output windowed aggregates over the expandable substrate |
| #61 | 59/59 | fix(nebula): Fix three root causes preventing windowed-aggregate query plans from deserializing |
| #62 | 59/59 | feat(nebula): Add tnpoint network-constrained windowed aggregates |
| #63 | 59/59 | fix(harness): Resolve systest tokens for every operator family in the proven callable count |
| #64 | 59/59 | feat(nebula): Add unary temporal-transform value-output windowed aggregates |
| #65 | 59/59 | fix(nebula): Initialize MEOS per worker thread to fix nondeterministic null-timezone segfault |
| #66 | 59/59 | feat(nebula): Add geometry value-output windowed aggregates |
| #67 | 59/59 | feat(nebula): Add cross-vehicle STBox predicates as per-event operators |
| #68 | 59/59 | perf(nebula): Convert extent and union aggregates to incremental MEOS-accumulator slots |
| #69 | 59/59 | feat(nebula): Add cross-vehicle tnumber-vs-tnumber position and topological predicates |
| #70 | 59/59 | feat(nebula): Add cross-vehicle tnpoint and tpose binary scalar predicates |
| #71 | 59/59 | feat(nebula): Add cross-vehicle Temporal*-returning combinators over two per-vehicle trajectories |
| #187 | 59/59 | feat(codegen): recreate aggregation descriptor + wire codegen_aggregations.py (surface reproduces committed byte-for-byte) |
| #188 | 59/59 | fix(nebula): name the multiply operator MUL to derive from canonical mul_tnumber_tnumber |
| #189 | 58/58 | ci(nebula): add the codegen drift-guard for the windowed-aggregation surface |

## Partial — the generator emits some of what these carry

| PR | emitted | carried | title |
|---|---|---|---|
| #192 | 20 | 21 | Generate trgeometry spatial-predicate operators |
| #168 | 54 | 97 | feat(meos): add trgeometry vs geometry spatial predicate NES operators (W148) |
| #111 | 44 | 98 | feat(meos): add TLT_{FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W91) |
| #110 | 44 | 98 | feat(meos): add TLE_{FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W90) |
| #109 | 44 | 98 | feat(meos): add TGT_{FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W89) |
| #108 | 44 | 98 | feat(meos): add TGE_{FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W88) |
| #107 | 44 | 98 | feat(meos): add TNE_{BOOL_TBOOL,TBOOL_BOOL,FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W87) |
| #106 | 44 | 98 | feat(meos): add TEQ_{BOOL_TBOOL,TBOOL_BOOL,FLOAT_TFLOAT,TFLOAT_FLOAT,INT_TINT,TINT_INT,TEMPORAL_TEMPORAL} NES operators (W86) |
| #105 | 44 | 98 | feat(meos): add TNOT_TBOOL, TAND_{BOOL_TBOOL,TBOOL_BOOL,TBOOL_TBOOL}, TOR_{BOOL_TBOOL,TBOOL_BOOL,TBOOL_TBOOL} NES operators (W85) |
| #104 | 44 | 98 | feat(meos): add EVER/ALWAYS_{EQ,GE,GT,LE,LT,NE}_TEMPORAL_TEMPORAL NES operators (W83-W84) |
| #96 | 43 | 98 | feat(meos): add EVER_EQ/GE/GT/LE/LT/NE_TFLOAT_TFLOAT NES operators (W67) |
| #103 | 40 | 98 | feat(meos): add EVER/ALWAYS_EQ/NE_{TBOOL_BOOL,BOOL_TBOOL} NES operators (W79-W82) |
| #97 | 40 | 98 | feat(meos): add ALWAYS_EQ/GE/GT/LE/LT/NE_TFLOAT_TFLOAT NES operators (W68) |
| #44 | 23 | 58 | Streaming MEOS-parity harness and NebulaStream adapter |
| #98 | 37 | 98 | feat(meos): add EVER_EQ/GE/GT/LE/LT/NE_TINT_TINT NES operators (W69) |
| #99 | 32 | 98 | feat(meos): add ALWAYS_EQ/GE/GT/LE/LT/NE_TINT_TINT NES operators (W70) |
| #194 | 5 | 16 | Generate temporal-to-temporal transform operators via hex-WKB serialization |
| #100 | 29 | 98 | feat(meos): add EVER_EQ/GE/GT/LE/LT/NE_TBIGINT_TBIGINT NES operators (W71) |
| #101 | 27 | 98 | feat(meos): add ALWAYS_EQ/GE/GT/LE/LT/NE_TBIGINT_TBIGINT NES operators (W72) |

## Uncovered — the generator emits none of these, so they are the work it owes

| PR | operators | title |
|---|---|---|
| #42 | 62 | Add the temporal circular buffer nearest-approach distance operators |
| #41 | 62 | Add the temporal pose and network point dwithin operators |
| #40 | 62 | Add the temporal pose and network point nearest-approach distance operators |
| #39 | 62 | Add the temporal network point spatial-relationship operators |
| #38 | 62 | Add the temporal pose–pose spatial-relationship operators |
| #37 | 62 | Add the temporal pose–geometry spatial-relationship operators |
| #36 | 59 | Add the temporal circular buffer dwithin operators |
| #43 | 57 | Convert the base MEOS systests to the runnable DDL format |
| #35 | 53 | Add the temporal circular buffer–circular buffer spatial-relationship operators |
| #34 | 47 | Add the temporal circular buffer–buffer spatial-relationship operators |
| #33 | 38 | Add the temporal circular buffer–geometry spatial-relationship operators |
| #32 | 28 | Add the temporal geo scalar accessor operators |
| #31 | 28 | Add the temporal number average and time-weighted average aggregations |
| #30 | 28 | Add windowed aggregation operators and the aggregation generator |
| #29 | 28 | Add the temporal geo restriction operators |
| #28 | 26 | Add the temporal number nearest-approach distance operators |
| #191 | 25 | Generate tbigint temporal-number comparison operators |
| #27 | 22 | Auto-inject parser glue for the generated operators |
| #26 | 22 | Add the temporal distance operators |
| #25 | 17 | Add the temporal geo–geo spatial-relationship operators |
| #193 | 10 | Generate geo-first tgeo spatial-predicate operators |
| #24 | 8 | Complete the temporal geo–geometry spatial-relationship operators |
| #23 | 6 | Add the temporal geo–geometry spatial-relationship operators |
| #196 | 5 | Generate temporal-vs-geometry transform operators via hex-WKB serialization |
| #195 | 5 | Generate scalar-first tbigint arithmetic operators via hex-WKB serialization |
| #197 | 3 | Generate trgeometry stbox-restriction operators via hex-WKB serialization |
| #175 | 1 | Add the MobilityNebula pin manifest and per-binding generator policy |
| #171 | 1 | fix(ci): make the Nix build work with NES_ENABLE_MEOS=OFF and stock paho-mqtt-cpp |
| #170 | 1 | feat(codegen): IDL-driven NES MEOS-operator generator with idempotent build/grammar/QPC glue |
| #72 | 1 | fix(ci): skip MQTT plugins in the Nix build where PahoMqttCpp is absent |
| #22 | 1 | Add proto extra fields and fix unused-parameter warnings in the aggregations |
| #21 | 1 | Add the MEOS-operator generator for the NebulaStream codegen path |
| #20 | 1 | Parameterize the cross-distance vehicle pair via SQL arguments |
| #19 | 1 | Parameterize the pair-meeting distance threshold via a SQL argument |
| #18 | 1 | Add the BerlinMOD streaming-semantics tier overlay |
| #17 | 1 | Add the pair-meeting and cross-distance aggregations |
| #16 | 1 | Add the temporal length aggregation |
| #15 | 1 | Add the BerlinMOD nine-query three-form parity matrix for NebulaStream |

## Carrying no operator file

| PR | title |
|---|---|
| #190 | Build the libmeos dependency image with all MEOS families via -DALL |

