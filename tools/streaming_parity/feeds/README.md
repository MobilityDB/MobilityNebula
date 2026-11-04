# Measured feeds — the reproducible artifact behind the parity numbers

A *feed* is `<meos_function><TAB>{proven|wired}`, the input to
`streaming_parity.py` and `ci_gate.py`. A committed feed reproduces the
assessment table in seconds without re-running the callability harness.

## `streamable.txt`

The 1,938 streamable MEOS public functions (tiers `stateless`, `bounded-state`,
`windowed`, `cross-stream`) of the streaming-relevance baseline, one per line.
`type-catalog.txt` lists the type-catalog / introspection functions the JMEOS
binding carries outside the streaming facade.

## `flink-kafka.feed.tsv`

Flink and Kafka share one generated JNR-FFI facade (`MeosOps*.java`), so a
single feed covers both. It is measured against the `accumulate/parity-1.4`
libmeos (extended-type C-API present), with `LD_LIBRARY_PATH` pinned to that
build.

- `proven` (2,208 rows) = the per-method callability harness invoked the method
  on real libmeos without a binding failure.
- `wired` (24 rows) = the facade method exists and is not confirmed callable.

Reproduce the table:

```sh
python3 ../streaming_parity.py \
  --catalog <streaming-relevance-baseline.json> \
  --platform Flink/Kafka --feed flink-kafka.feed.tsv
```

Regenerate the feed (binds every method on real libmeos, minutes with the
parallel runner):

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
under test, not a system `/usr/local/lib/libmeos.so`.

## `nebula.feed.tsv`

The NebulaStream feed of this checkout: `adapters/nebula.py --root .` over the
six MEOS physical functions under `nes-physical-operators/src/Functions/Meos/`
and `nes-plugins/MEOS/MEOSWrapper.cpp`. It holds 9 `wired` rows and no `proven`
row, because the checkout carries no `nes-systests/function/meos/` systest.
