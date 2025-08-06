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
    # Use core2 CPU target which is more conservative than x86-64
    export RUSTFLAGS="-C target-cpu=core2 -C target-feature=-sse3,-ssse3,-sse4.1,-sse4.2,-popcnt,-avx,-avx2,-fma,-bmi1,-bmi2,-lzcnt,-movbe -C opt-level=2 -C lto=false"

    # Set conservative compiler flags for Python native extensions
    export CFLAGS="-O2 -march=core2 -mtune=generic -mno-sse3 -mno-ssse3 -mno-sse4.1 -mno-sse4.2 -mno-popcnt -mno-avx -mno-avx2"
    export CXXFLAGS="-O2 -march=core2 -mtune=generic -mno-sse3 -mno-ssse3 -mno-sse4.1 -mno-sse4.2 -mno-popcnt -mno-avx -mno-avx2"
    export LDFLAGS="-Wl,-O1"

    # Use ultra-conservative PyApp build with maximum CPU compatibility
    echo "Building with ultra-conservative PyApp for maximum CPU compatibility..."
    hatch -e packaging run build-isolated-binary

    # Debug: list what files are actually in the binary directory
    echo "Contents of $DIST_DIR/binary:"
    ls -la $DIST_DIR/binary/ || echo "Binary directory not found"

    # Find the actual binary file (it might not have the expected name)
    BINARY_FILE=$(find $DIST_DIR/binary -name "snow*" -type f | head -1)
    if [[ -z "$BINARY_FILE" ]]; then
      echo "Error: No binary file found in $DIST_DIR/binary/"
      exit 1
    fi

    echo "Found binary: $BINARY_FILE"
    mkdir -p $DIST_DIR/snow
    mv "$BINARY_FILE" $DIST_DIR/snow/snow

    echo "PyApp build completed successfully"
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
