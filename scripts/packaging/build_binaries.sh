#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
ROOT_DIR="$(git rev-parse --show-toplevel)"
BUILD_DIR="${ROOT_DIR}/build"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)
ENTRY_POINT="src/snowflake/cli/_app/__main__.py"

clean_build_workspace() {
  rm -rf $DIST_DIR $BUILD_DIR || true
}

build_binaries() {
  if [[ ${SYSTEM} == "darwin" ]]; then
    echo "Building for Darwin moved to build_darwin_package.sh"
    exit 0
  elif [[ ${SYSTEM} == "linux" ]]; then
    hatch -e packaging run pyinstaller \
      --name=snow \
      --target-architecture=$MACHINE \
      --onedir \
      --clean \
      --noconfirm \
      --contents-directory=snowflake-cli-${VERSION} \
      ${ENTRY_POINT}
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

clean_build_workspace
build_binaries
execute_build
