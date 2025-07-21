#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
ROOT_DIR="$(git rev-parse --show-toplevel)"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)

# Set Rust compiler flags for broader CPU compatibility
# Use generic x86-64 baseline to ensure compatibility with older processors
# This avoids newer instructions that may not be available on older CPUs
export RUSTFLAGS="-C target-cpu=generic"

# Configure Python distribution source for better CPU compatibility
# Use python-build-standalone distributions which are more conservatively compiled
export HATCH_PYTHON_SOURCE_3_10="https://github.com/indygreg/python-build-standalone/releases/download/20241002/cpython-3.10.15+20241002-x86_64-unknown-linux-gnu-install_only.tar.gz"

install_cargo() {
  curl https://sh.rustup.rs -sSf > rustup-init.sh
  bash rustup-init.sh -y
  . $HOME/.cargo/env
  rm rustup-init.sh
}

clean_build_workspace() {
  rm -rf $DIST_DIR || true
}

build_binaries() {
  if [[ ${SYSTEM} == "darwin" ]]; then
    echo "Building for Darwin moved to build_darwin_package.sh"
    exit 0
  elif [[ ${SYSTEM} == "linux" ]]; then
    hatch -e packaging run build-isolated-binary
    mkdir $DIST_DIR/snow
    mv $DIST_DIR/binary/snow-${VERSION} $DIST_DIR/snow/snow
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
