#!/usr/bin/env bash
# regen-from-pin.sh — regenerate the MobilityNebula NES MEOS operators from the catalog
# (per GENERATION.md).
#
# Usage:  tools/regen-from-pin.sh <pin>
#   env:  CATALOG = path to meos-idl.json produced by MEOS-API run.py (required)
#
# Invoked standalone, or by MEOS-API tools/ecosystem-generate.sh in dependency order.
set -euo pipefail
PIN="${1:?usage: regen-from-pin.sh <pin>}"
CATALOG="${CATALOG:?set CATALOG to the meos-idl.json from MEOS-API run.py}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

# 1. vendor the catalog for the in-repo generator
cp "$CATALOG" "$HERE/tools/codegen/meos-idl.json"

# 2. run the in-repo generator (tools/codegen/codegen_nebula.py) -> the NES MEOS-operator surface
#    + build/grammar/QPC glue. The codegen input descriptor follows tools/codegen/codegen_input.example.json.
( cd "$HERE" && python3 tools/codegen/codegen_nebula.py \
    --input tools/codegen/codegen_input.example.json \
    --output-root . )

# 3. build-verify the generated operators (the in-repo containerized build)
( cd "$HERE" && bash tools/codegen/build_local.sh ) || echo "WARN: MobilityNebula build returned non-zero"
echo "[nebula] regenerated NES operators from catalog at pin $PIN"
