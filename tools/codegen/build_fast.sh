#!/usr/bin/env bash
#
# Fast systest build for MobilityNebula.
#
# The dev image already wires ccache as the compiler launcher
# (USE_CCACHE_IF_AVAILABLE=ON), but its cache lives at /root/.cache/ccache
# INSIDE the container, so every `docker run --rm` discards it and starts cold
# (~20 min for a grammar change that recompiles the operator libs).
#
# This wrapper makes builds fast by:
#   - persisting the ccache on the HOST (survives across container runs) so an
#     unchanged TU is an instant cache hit -> a token rename rebuilds only the
#     parser TU + relinks instead of ~1500 operator TUs;
#   - a large cache + relaxed sloppiness (time macros / include mtime) so the
#     hit rate approaches 100% on no-op-for-operators changes;
#   - CCACHE_BASEDIR so the cache is independent of the checkout path;
#   - mold as the linker (one-time reconfigure) for fast relinks.
#
# Usage:  tools/codegen/build_fast.sh [target]        # default: systest
#         JOBS=8 tools/codegen/build_fast.sh
set -euo pipefail
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
IMAGE="${NES_DEV_IMAGE:-localhost/nes-development:mobilitynebula-v6}"
BUILD_DIR="${BUILD_DIR:-build-w15}"
TARGET="${1:-systest}"
JOBS="${JOBS:-6}"
HOST_CCACHE="${HOST_CCACHE:-$HOME/.nebula-ccache}"
mkdir -p "$HOST_CCACHE"

/usr/bin/docker run --rm \
  -v "$ROOT":/workspace -w /workspace \
  -v "$HOST_CCACHE":/ccache \
  -e CCACHE_DIR=/ccache \
  -e CCACHE_BASEDIR=/workspace \
  -e CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-40G}" \
  -e CCACHE_SLOPPINESS=time_macros,include_file_mtime,include_file_ctime,pch_defines,locale \
  -e CCACHE_COMPILERCHECK=content \
  "$IMAGE" bash -lc '
    set -e
    # Seed the empty host cache from the image-baked cache once.
    if [ -z "$(ls -A /ccache 2>/dev/null)" ] && [ -d /root/.cache/ccache ]; then
      cp -a /root/.cache/ccache/. /ccache/ 2>/dev/null || true
    fi
    # One-time: switch the linker to mold (only rewrites link steps).
    if ! grep -q "CMAKE_LINKER_TYPE:STRING=MOLD" '"$BUILD_DIR"'/CMakeCache.txt 2>/dev/null; then
      cmake -S . -B '"$BUILD_DIR"' -DCMAKE_LINKER_TYPE=MOLD >/dev/null
    fi
    ccache -z >/dev/null 2>&1 || true
    ninja -j '"$JOBS"' -C '"$BUILD_DIR"' '"$TARGET"'
    echo "=== ccache stats (this build) ==="
    ccache -s | grep -iE "hits|miss|cacheable" | head
  '
