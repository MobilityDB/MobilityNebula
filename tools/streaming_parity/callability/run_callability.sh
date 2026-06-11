#!/usr/bin/env bash
# Callability test driver for the Flink/Kafka MEOS facade (shared MeosOps* surface).
# Compiles the facade against JMEOS, then runs FacadeCallability ONE class per JVM
# (crash isolation) with libmeos on the JNR-FFI path, aggregating the callable set.
#
# Usage: run_callability.sh <facade-src-dir> <pkg> <jmeos-fat.jar> <libmeos-dir> [out.txt]
#   e.g. run_callability.sh \
#         flink-processor/src/main/java/org/mobilitydb/flink/meos \
#         org.mobilitydb.flink.meos /path/JMEOS-fat.jar /usr/local/lib callable.txt
set -euo pipefail
FAC="$1"; PKG="$2"; JAR="$3"; LIBDIR="$4"; OUT="${5:-callable.txt}"
HERE="$(cd "$(dirname "$0")" && pwd)"
# JMEOS loads libmeos via jnr `LibraryLoader.search(<user.dir>/src).load("meos")`,
# which resolves through the OS loader — it does NOT honour
# -Djnr.ffi.library.path. So the lib actually measured is whatever the OS finds
# (e.g. a stale /usr/local/lib/libmeos.so), NOT $LIBDIR, unless we put $LIBDIR on
# LD_LIBRARY_PATH. Pin it so the harness measures the build under test.
export LD_LIBRARY_PATH="$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
WORK="$(mktemp -d)"
javac -cp "$JAR" -d "$WORK" "$FAC"/*.java
javac -cp "$JAR:$WORK" -d "$WORK" "$HERE/FacadeCallability.java"
: > "$OUT"
for cls in $(ls "$WORK"/$(echo "$PKG" | tr . /)/MeosOps*.class | xargs -n1 basename | sed 's/\.class//' | grep -v '\$'); do
  timeout 30 java -cp "$WORK:$JAR" -Djnr.ffi.library.path="$LIBDIR" \
    FacadeCallability "$PKG" "$cls" "$OUT" >/dev/null 2>&1 || true   # abort = reached MEOS
done
sort -u "$OUT" -o "$OUT"
echo "CALLABLE: $(wc -l < "$OUT")"

# --- Per-method variant (rigorous; the DEFINITIVE run) ---------------------
# FacadeCallability tests a whole class per JVM (a mid-class abort loses the
# rest, so it is only a fast floor). PerMethodCallability tests ONE
# (class,method) per JVM with TYPE-AWARE input synthesis (the "connectors":
# each Pointer arg built from a per-type sample inferred from the function-name
# tokens — see PerMethodCallability.java) and classifies by outcome:
#   reached native MEOS = CALLABLE — stdout OK | MEOSERR, native exit(1) on a
#     MEOS semantic error (rc!=0 with no Java stack trace), or a signal (rc>128);
#   stdout BINDFAIL (UnsatisfiedLink/NoSuchMethod/marshalling) = NOT callable;
#   stdout NOMETHOD/NOSYNTH = untestable here (args not literal-synthesizable).
# This run lifted confirmed Flink/Kafka callability to 1472 of 1949 streamable
# (75.5%). The remainder splits into (a) extended-type ops whose ctor is ABSENT
# from the linked libmeos (cbuffer/pose/tcbuffer/tpose/trgeo, ~302) — a reason-
# marked, nm -D-provable gap pending the extended-type C-API, NOT a binding
# defect; and (b) ~173 present-in-libmeos ops whose args are arrays / aggregate
# state / type-enums (not literal-synthesizable), correctness inherited from the
# MEOS PostgreSQL suite. Three are declared-not-built defects (tfloat_avg_value,
# tnumber_trend, geog_from_binary).
run_per_method() {  # <facade-dir> <pkg> <jmeos.jar> <libmeos-dir> [out.txt]
  local FAC="$1" PKG="$2" JAR="$3" LIBDIR="$4" OUT="${5:-callable.txt}"
  local HERE WORK ALL ERR; HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  export LD_LIBRARY_PATH="$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"  # pin lib under test (see top note)
  WORK="$(mktemp -d)"; ERR="$(mktemp)"
  javac -cp "$JAR" -d "$WORK" "$FAC"/*.java
  javac -cp "$JAR:$WORK" -d "$WORK" "$HERE/PerMethodCallability.java"
  ALL=$(ls "$WORK"/$(echo "$PKG" | tr . /)/MeosOps*.class | xargs -n1 basename \
        | sed 's/\.class//' | grep -vE '\$|Runtime' | tr '\n' ',' | sed 's/,$//')
  : > "$OUT"; : > "$OUT.not"; : > "$OUT.skip"
  for cls in $(echo "$ALL" | tr ',' ' '); do
    for m in $(javap -p -classpath "$WORK:$JAR" "$PKG.$cls" 2>/dev/null \
               | grep -E "public static" \
               | sed -E 's/.*\b([a-z][a-zA-Z0-9_]*)\(.*/\1/' | grep -E '^[a-z]'); do
      out=$(timeout 25 java -cp "$WORK:$JAR" -Djnr.ffi.library.path="$LIBDIR" \
            PerMethodCallability "$PKG" "$cls" "$m" "$ALL" 2>"$ERR"); rc=$?
      v=$(printf '%s\n' "$out" | grep -E '^(OK|MEOSERR|BINDFAIL|NOMETHOD|NOSYNTH)$' | tail -1)
      jt=$(grep -cE '^[[:space:]]+at [A-Za-z]' "$ERR")
      if [ "$v" = OK ] || [ "$v" = MEOSERR ] || [ "$rc" -gt 128 ]; then echo "$m" >> "$OUT"
      elif [ "$v" = BINDFAIL ]; then echo "$m" >> "$OUT.not"
      elif [ "$v" = NOMETHOD ] || [ "$v" = NOSYNTH ]; then echo "$m" >> "$OUT.skip"
      elif [ "$rc" -ne 0 ] && [ "$jt" -eq 0 ]; then echo "$m" >> "$OUT"  # native MEOS exit
      else echo "$m" >> "$OUT.skip"; fi
    done
  done
  sort -u "$OUT" -o "$OUT"; sort -u "$OUT.not" -o "$OUT.not"; sort -u "$OUT.skip" -o "$OUT.skip"
  echo "CALLABLE=$(wc -l < "$OUT") BINDFAIL=$(wc -l < "$OUT.not") SKIP=$(wc -l < "$OUT.skip")"
}

# --- Parallel per-method variant (same classification, N JVMs at once) --------
# One JVM per (class,method) is correct but ~30 min serially over 2,097 methods.
# Each JVM is independent (crash isolation already per-method), so they fan out
# cleanly. NPROC defaults to 6. Produces identical $OUT/$OUT.not/$OUT.skip.
# Usage: run_per_method_par <facade-dir> <pkg> <jmeos.jar> <libmeos-dir> [out.txt] [nproc]
run_per_method_par() {
  local FAC="$1" PKG="$2" JAR="$3" LIBDIR="$4" OUT="${5:-callable.txt}" NP="${6:-6}"
  local HERE WORK ALL; HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  export LD_LIBRARY_PATH="$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"  # pin lib under test
  WORK="$(mktemp -d)"
  javac -cp "$JAR" -d "$WORK" "$FAC"/*.java
  javac -cp "$JAR:$WORK" -d "$WORK" "$HERE/PerMethodCallability.java"
  ALL=$(ls "$WORK"/$(echo "$PKG" | tr . /)/MeosOps*.class | xargs -n1 basename \
        | sed 's/\.class//' | grep -vE '\$|Runtime' | tr '\n' ',' | sed 's/,$//')
  # one (class method) pair per line
  local WL="$WORK/worklist"; : > "$WL"
  local cls m
  for cls in $(echo "$ALL" | tr ',' ' '); do
    for m in $(javap -p -classpath "$WORK:$JAR" "$PKG.$cls" 2>/dev/null \
               | grep -E "public static" \
               | sed -E 's/.*\b([a-z][a-zA-Z0-9_]*)\(.*/\1/' | grep -E '^[a-z]'); do
      echo "$cls $m" >> "$WL"
    done
  done
  # per-pair classifier emitted to a temp script (xargs can't see bash functions)
  local PR="$WORK/prun.sh"
  cat > "$PR" <<EOF
#!/usr/bin/env bash
out=\$(timeout 25 java -cp "$WORK:$JAR" PerMethodCallability "$PKG" "\$1" "\$2" "$ALL" 2>/tmp/e.\$\$); rc=\$?
v=\$(printf '%s\n' "\$out" | grep -E '^(OK|MEOSERR|BINDFAIL|NOMETHOD|NOSYNTH)\$' | tail -1)
jt=\$(grep -cE '^[[:space:]]+at [A-Za-z]' /tmp/e.\$\$); rm -f /tmp/e.\$\$
if [ "\$v" = OK ] || [ "\$v" = MEOSERR ] || [ "\$rc" -gt 128 ]; then echo "CALLABLE \$2"
elif [ "\$v" = BINDFAIL ]; then echo "BINDFAIL \$2"
elif [ "\$v" = NOMETHOD ] || [ "\$v" = NOSYNTH ]; then echo "SKIP \$2"
elif [ "\$rc" -ne 0 ] && [ "\$jt" -eq 0 ]; then echo "CALLABLE \$2"
else echo "SKIP \$2"; fi
EOF
  chmod +x "$PR"
  local RES="$WORK/res"; xargs -P "$NP" -L 1 "$PR" < "$WL" > "$RES"
  awk '$1=="CALLABLE"{print $2}' "$RES" | sort -u > "$OUT"
  awk '$1=="BINDFAIL"{print $2}' "$RES" | sort -u > "$OUT.not"
  awk '$1=="SKIP"{print $2}'     "$RES" | sort -u > "$OUT.skip"
  echo "CALLABLE=$(wc -l < "$OUT") BINDFAIL=$(wc -l < "$OUT.not") SKIP=$(wc -l < "$OUT.skip")"
}
# run_per_method_par "$FAC" "$PKG" "$JAR" "$LIBDIR" callable.txt 6
