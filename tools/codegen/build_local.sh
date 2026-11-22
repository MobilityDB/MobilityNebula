#!/usr/bin/env bash
#
# Local build of MobilityNebula, compiled as CI compiles it: the steps of
# .github/workflows/container-build.yml in the project's Nix devShell, libmeos built from the
# MobilityDB commit nes-plugins/MEOS/MEOS_COMMIT names, with one compiler cache shared by every
# worktree.
#
# The devShell keeps its cache in the worktree (.ccache) unless CCACHE_DIR names one; this wrapper
# names ~/.cache/ccache-nes, and the devShell's CCACHE_BASEDIR makes the cached entries relative to
# the worktree, so a build in any worktree reuses what another compiled. libmeos is built once per
# MobilityDB commit, as CI keeps it.
#
# Usage:
#   tools/codegen/build_local.sh [target ...]        # default: every target
#
# Environment:
#   NES_MEOS_ROOT        an installed libmeos to build against (default: the MEOS_COMMIT one, built
#                        into ~/.cache/nes-meos/<commit> on first use)
#   NES_CCACHE_DIR       the shared compiler cache (default: ~/.cache/ccache-nes)
#   NES_CCACHE_MAXSIZE   its size limit (default: 20G)
#   BUILD_DIR            the build directory in the repository (default: cmake-build-local)
#   JOBS                 parallel compile jobs (default: the cores of the machine)
#   TESTS                the tests run after a build of every target, as container-build.yml's steps
#                        run them: "all" (default) runs the unit tests (-LE "Systest|DockerCompose"),
#                        then the system tests (-L Systest); "unit" the first alone; "none" neither
#   SYSTESTS             a ctest regular expression over the system-test runs, each running every
#                        .test file in one mode (systest_compiler_sliceCache_true, ...) (default: all)
#
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
export CCACHE_DIR="${NES_CCACHE_DIR:-$HOME/.cache/ccache-nes}"
export CCACHE_MAXSIZE="${NES_CCACHE_MAXSIZE:-20G}"
mkdir -p "$CCACHE_DIR"
BUILD_DIR="${BUILD_DIR:-cmake-build-local}"
JOBS="${JOBS:-$(nproc)}"

# libmeos of the MobilityDB commit the plugin is generated from, built in the devShell once per
# commit, as CI's "libmeos of MEOS_COMMIT" step builds it.
MEOS_ROOT="${NES_MEOS_ROOT:-}"
if [ -z "$MEOS_ROOT" ]; then
  sha=$(cat nes-plugins/MEOS/MEOS_COMMIT)
  MEOS_ROOT="$HOME/.cache/nes-meos/${sha:0:12}"
  # The devShell's CMake installs libmeos in lib64 (GNUInstallDirs), the Container Build's in lib
  if [ ! -f "$MEOS_ROOT/lib/libmeos.so" ] && [ ! -f "$MEOS_ROOT/lib64/libmeos.so" ]; then
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
  ./.nix/nix-cmake.sh -GNinja -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug -DNES_SPLIT_TEST_BINARIES=OFF \
    -DNES_ENABLE_MEOS=ON -DMEOS_ROOT="$MEOS_ROOT" | tee "$BUILD_DIR/configure.log"
  # The MEOS plugin is optional in CMake: without libmeos it is skipped and the build passes
  # without compiling a single MEOS source. Refuse that.
  if grep -q "Skipping optional plugin: MEOS" "$BUILD_DIR/configure.log"; then
    echo "the MEOS plugin was skipped; libmeos was not found at $MEOS_ROOT" >&2
    exit 1
  fi
fi
./.nix/nix-run.sh ccache --zero-stats > /dev/null
rc=0
if [ $# -gt 0 ]; then
  ./.nix/nix-cmake.sh --build "$BUILD_DIR" --target "$@" -j"$JOBS" -- -k 0 || rc=$?
else
  ./.nix/nix-cmake.sh --build "$BUILD_DIR" -j"$JOBS" -- -k 0 || rc=$?
fi
./.nix/nix-run.sh ccache --show-stats | grep -E 'Hits|Misses|Cache size'
echo "### BUILD EXIT=$rc"
# A pull request is pushed on both of CI's test steps, not on a subset of them: a build of named targets
# runs none, since the tests need every target.
TESTS="${TESTS:-all}"
if [ $# -gt 0 ]; then
  TESTS=none
fi
if [ "$rc" -eq 0 ] && [ "$TESTS" != "none" ]; then
  ./.nix/nix-run.sh bash -c "source .github/.env/test-none.env; ctest --test-dir '$BUILD_DIR' --no-tests=error \
    -j'$JOBS' -LE 'Systest|DockerCompose' --output-on-failure --timeout 2400" || rc=$?
  echo "### UNIT EXIT=$rc"
fi
if [ "$rc" -eq 0 ] && [ "$TESTS" = "all" ]; then
  ./.nix/nix-run.sh bash -c "source .github/.env/test-none.env; ctest --test-dir '$BUILD_DIR' --no-tests=error \
    -j4 -L Systest -R '${SYSTESTS:-.}' --output-on-failure --timeout 4800" || rc=$?
  echo "### SYSTEST EXIT=$rc"
fi
exit "$rc"
