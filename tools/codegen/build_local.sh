#!/usr/bin/env bash
#
# Local build of MobilityNebula, compiled exactly as CI compiles it: the steps of
# .github/workflows/nix-build.yml in the project's Nix devShell, libmeos built from MobilityDB
# master, with one compiler cache shared by every worktree.
#
# The devShell keeps its cache in the worktree (.ccache) unless CCACHE_DIR names one; this wrapper
# names ~/.cache/ccache-nes, and the devShell's CCACHE_BASEDIR makes the cached entries relative to
# the worktree, so a build in any worktree reuses what another compiled. libmeos is built once per
# MobilityDB commit, as CI caches it.
#
# Usage:
#   tools/codegen/build_local.sh [target ...]        # default: the 3 op/parser libs
#
# Environment:
#   NES_MEOS_ROOT        an installed libmeos to build against (default: MobilityDB master, built
#                        into ~/.cache/nes-meos/<commit> on first use)
#   NES_CCACHE_DIR       the shared compiler cache (default: ~/.cache/ccache-nes)
#   NES_CCACHE_MAXSIZE   its size limit (default: 20G)
#   BUILD_DIR            the build directory in the repository (default: cmake-build-local)
#   BUILD_TYPE           CMAKE_BUILD_TYPE of a new configure (default: Debug, as CI's Nix build)
#   JOBS                 parallel compile jobs (default: the cores of the machine)
#
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
export CCACHE_DIR="${NES_CCACHE_DIR:-$HOME/.cache/ccache-nes}"
export CCACHE_MAXSIZE="${NES_CCACHE_MAXSIZE:-20G}"
mkdir -p "$CCACHE_DIR"
BUILD_DIR="${BUILD_DIR:-cmake-build-local}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
JOBS="${JOBS:-$(nproc)}"
TARGETS="${*:-nes-physical-operators nes-logical-operators nes-sql-parser}"

# libmeos of MobilityDB master, built in the devShell once per commit, as CI's "Build libmeos
# from MobilityDB master via devShell" step builds it.
MEOS_ROOT="${NES_MEOS_ROOT:-}"
if [ -z "$MEOS_ROOT" ]; then
  sha=$(git ls-remote https://github.com/MobilityDB/MobilityDB.git refs/heads/master | cut -f1)
  MEOS_ROOT="$HOME/.cache/nes-meos/${sha:0:12}"
  if [ ! -f "$MEOS_ROOT/lib/libmeos.so" ]; then
    src="$HOME/.cache/nes-meos/src-${sha:0:12}"
    rm -rf "$src" "$src-build"
    git init -q "$src"
    git -C "$src" fetch -q --depth 1 https://github.com/MobilityDB/MobilityDB.git "$sha"
    git -C "$src" checkout -q FETCH_HEAD
    ./.nix/nix-run.sh cmake -S "$src" -B "$src-build" -G Ninja \
      -DMEOS=ON -DALL=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$MEOS_ROOT"
    ./.nix/nix-run.sh cmake --build "$src-build" -j"$JOBS"
    ./.nix/nix-run.sh cmake --install "$src-build"
    rm -rf "$src" "$src-build"
  fi
fi

echo "### build=$BUILD_DIR meos=$MEOS_ROOT ccache=$CCACHE_DIR jobs=$JOBS"
if [ ! -f "$BUILD_DIR/build.ninja" ]; then
  mkdir -p "$BUILD_DIR"
  ./.nix/nix-cmake.sh -DCMAKE_BUILD_TYPE="$BUILD_TYPE" -G Ninja -S . -B "$BUILD_DIR" \
    -DNES_ENABLE_MEOS=ON -DMEOS_ROOT="$MEOS_ROOT" | tee "$BUILD_DIR/configure.log"
  # The MEOS plugin is optional in CMake: without libmeos it is skipped and the build passes
  # without compiling a single MEOS source. Refuse that, as CI does.
  if grep -q "Skipping optional plugin: MEOS" "$BUILD_DIR/configure.log"; then
    echo "the MEOS plugin was skipped; libmeos was not found at $MEOS_ROOT" >&2
    exit 1
  fi
fi
./.nix/nix-run.sh ccache --zero-stats > /dev/null
rc=0
./.nix/nix-cmake.sh --build "$BUILD_DIR" --target $TARGETS -j"$JOBS" -- -k 0 || rc=$?
./.nix/nix-run.sh ccache --show-stats | grep -E 'Hits|Misses|Cache size'
echo "### EXIT=$rc"
exit "$rc"
