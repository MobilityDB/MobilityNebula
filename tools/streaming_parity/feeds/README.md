# Measured feeds — the reproducible artifact behind the parity numbers

A *feed* is `<meos_function><TAB>{proven|wired}`, the input to
`streaming_parity.py`. Committing the feed makes the assessment table
reproducible in seconds without re-running the multi-minute callability harness.

## `flink-kafka.feed.tsv`

Flink and Kafka share one generated JNR-FFI facade (`MeosOps*.java`, 2,097
`public static` methods), so a single feed covers both. Measured against the
**`accumulate/parity-1.4` libmeos** (extended-type C-API present), with
`LD_LIBRARY_PATH` pinned to that build.

- `proven` = the per-method callability harness invoked it on real libmeos
  without a binding failure (2142 facade methods).
- `wired`  = facade method exists but not confirmed callable (24).

> **Note on the 65 `trgeometry_*` entries:** these were measured against **JMEOS
> PR #19**'s renamed `GeneratedFunctions` (the facade is a thin pass-through, so
> `GeneratedFunctions.trgeometry_round` == what the facade method runs).
> Regenerating the MobilityFlink/Kafka facade so it *emits* the `trgeometry_*`
> names (it currently still emits the old `trgeo_*`) is the pending mechanical
> step to make the shipped jar match this feed.

Reproduce the table (instant):

```sh
python3 ../streaming_parity.py \
  --catalog <streaming-relevance-baseline.json> \
  --platform Flink/Kafka --feed flink-kafka.feed.tsv
# => L3 CALLABLE 1,945 / 1,945 = 100.0%
```

(The catalog is the streaming-relevance baseline — the per-function tier map,
shared across platforms; see `../../streaming_parity.py` header.)

Regenerate the feed from scratch (binds every method on real libmeos, ~minutes
with the parallel runner):

```sh
source ../callability/run_callability.sh
run_per_method_par \
  <MobilityFlink>/flink-processor/src/main/java/org/mobilitydb/flink/meos \
  org.mobilitydb.flink.meos <JMEOS-fat.jar> <dir-with-libmeos.so> callable.txt 6
python3 ../adapters/jvm_facade.py --facade-dir <…/meos> --passing callable.txt > flink-kafka.feed.tsv
```

**Pin the libmeos.** JMEOS resolves the library through the OS loader and
ignores `-Djnr.ffi.library.path`; `run_callability.sh` exports
`LD_LIBRARY_PATH=<dir-with-libmeos.so>` so the measurement reflects the build
under test, not a stale system `/usr/local/lib/libmeos.so`.
