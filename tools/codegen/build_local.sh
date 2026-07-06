#!/usr/bin/env bash
#
# Local compile-check of the generated MEOS operator + parser libraries in the
# NebulaStream dev image.
#
# The dev image ships without PahoMqttCpp (CI's image has it), so a configure
# that activates the optional MQTT Source/Sink plugins fails. Those plugins are
# irrelevant to the MEOS operator libraries, so this wrapper disables them for
# the local configure and ALWAYS restores nes-plugins/CMakeLists.txt afterwards
# (trap on EXIT) — no manual sed, and the working tree is never left modified
# even if the build fails or is interrupted.
#
# Usage:
#   tools/codegen/build_local.sh [target ...]        # default: the 3 op/parser libs
#   NES_DEV_IMAGE=... BUILD_DIR=... tools/codegen/build_local.sh
#
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
PLUGINS="$ROOT/nes-plugins/CMakeLists.txt"
IMAGE="${NES_DEV_IMAGE:-localhost/nes-development:mobilitynebula-v3}"
BUILD_DIR="${BUILD_DIR:-build-w15}"
TARGETS="${*:-nes-physical-operators nes-logical-operators nes-sql-parser}"

restore() { git -C "$ROOT" checkout -- "$PLUGINS" 2>/dev/null || true; }
trap restore EXIT

# Disable the optional MQTT plugins for the local configure (absent in this image).
sed -i 's#"Sources/MQTTSource" ON)#"Sources/MQTTSource" OFF)#; s#"Sinks/MQTTSink" ON)#"Sinks/MQTTSink" OFF)#' "$PLUGINS"

podman run --rm -v "$ROOT":/workspace -w /workspace "$IMAGE" \
  bash -lc "cmake --build '$BUILD_DIR' --target $TARGETS -j 8 -- -k 0; echo \"### EXIT=\$?\""
