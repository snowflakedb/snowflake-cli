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
}

clean_build_workspace() {
  rm -rf $DIST_DIR || true
}

build_debug_binaries() {
  if [[ ${SYSTEM} == "darwin" ]]; then
    echo "Building debug version for Darwin..."
    hatch -e packaging run build-debug-binary
    mkdir -p $DIST_DIR/snow-debug
    mv $DIST_DIR/binary/snow-${VERSION} $DIST_DIR/snow-debug/snow-debug
  elif [[ ${SYSTEM} == "linux" ]]; then
    echo "Building debug version for Linux..."
    hatch -e packaging run build-debug-binary
    mkdir -p $DIST_DIR/snow-debug
    mv $DIST_DIR/binary/snow-${VERSION} $DIST_DIR/snow-debug/snow-debug
  else
    echo "Unsupported platform: ${SYSTEM}"
    exit 1
  fi
}

execute_build() {
  echo "Executing debug build test"
  if [[ ${SYSTEM} == "linux" ]] || [[ ${SYSTEM} == "darwin" ]]; then
    echo "Debug binary built successfully!"
    echo "Location: $DIST_DIR/snow-debug/snow-debug"
    echo ""
    echo "To debug with gdb:"
    echo "  gdb $DIST_DIR/snow-debug/snow-debug"
    echo ""
    echo "To run the debug binary:"
    echo "  $DIST_DIR/snow-debug/snow-debug --help"
    echo ""
    echo "Note: This debug binary includes debugging symbols and may be slower than the release version."
  else
    echo "Unsupported platform: ${SYSTEM}"
    exit 1
  fi
}

install_cargo
clean_build_workspace
build_debug_binaries
execute_build
