#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
ROOT_DIR="$(git rev-parse --show-toplevel)"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)

install_cargo() {
  curl https://sh.rustup.rs -sSf > rustup-init.sh
  bash rustup-init.sh -y
  . $HOME/.cargo/env
  rm rustup-init.sh
  # Install x86_64 target for broader compatibility
  rustup target add x86_64-unknown-linux-gnu
}

clean_build_workspace() {
  rm -rf $DIST_DIR || true
}

build_binaries() {
  if [[ ${SYSTEM} == "darwin" ]]; then
    echo "Building for Darwin moved to build_darwin_package.sh"
    exit 0
  elif [[ ${SYSTEM} == "linux" ]]; then
    # Set environment variables for maximum x86_64 compatibility
    export CARGO_BUILD_TARGET="x86_64-unknown-linux-gnu"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=core2 -C target-feature=-avx,-avx2,-bmi1,-bmi2,-fma,-avx512f,-avx512dq,-avx512ifma,-avx512pf,-avx512er,-avx512cd,-avx512bw,-avx512vl,-avx512vbmi,-avx512vbmi2,-avx512vnni,-avx512bitalg,-avx512vpopcntdq,-avx512bf16,-avx512fp16,-avx512vp2intersect"

    # Set conservative compiler flags for Python native extensions
    export CFLAGS="-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma"
    export CXXFLAGS="$CFLAGS"
    export LDFLAGS="-Wl,-O1"

    # Use simple standalone approach for maximum CPU compatibility
    # PyApp binaries have persistent CPU instruction compatibility issues
    echo "Building with simple standalone approach for maximum CPU compatibility..."
    python scripts/packaging/build_simple_standalone.py

    # Copy the standalone version to expected location
    mkdir -p $DIST_DIR/snow
    cp -r $DIST_DIR/standalone/* $DIST_DIR/snow/

    echo "Standalone build completed successfully"
    echo "Contents of $DIST_DIR/snow:"
    ls -la $DIST_DIR/snow/
  else
    echo "Unsupported platform: ${SYSTEM}"
    exit 1
  fi
}

execute_build() {
  echo "Executing build"
  if [[ ${SYSTEM} == "linux" ]]; then
    $DIST_DIR/snow/snow --help
  else
    echo "Unsupported platform: ${SYSTEM}"
    exit 1
  fi
}

install_cargo
clean_build_workspace
build_binaries
execute_build
