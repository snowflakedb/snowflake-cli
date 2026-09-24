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
  rm -rf $DIST_DIR/binary $DIST_DIR/snow || true
  rm -f $DIST_DIR/snowflake-cli-*${MACHINE}.rpm $DIST_DIR/snowflake-cli-*${MACHINE}.deb || true
  rm -f $DIST_DIR/snowflake-cli-*.tar.gz $DIST_DIR/manifest-*.json || true
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

# Opt-in second output: snowflake-managed tarball + manifest fragment.
# BINARY snow at $DIST_DIR/snow/snow is left untouched. Releng sets this in PR-H.
build_snowflake_managed_tarball() {
  if [[ "${BUILD_SNOWFLAKE_MANAGED_TARBALL:-}" != "1" ]]; then
    return 0
  fi
  echo "Building snowflake-managed tarball"
  SNOWFLAKE_CLI_INSTALLATION_SOURCE=SNOWFLAKE_MANAGED \
    hatch -e packaging run build-isolated-binary
  case "${MACHINE}" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *)
      echo "Unsupported machine for managed tarball: ${MACHINE}"
      exit 1
      ;;
  esac
  test -f "${DIST_DIR}/snowflake-cli-${VERSION}-linux-${arch}.tar.gz"
  test -f "${DIST_DIR}/manifest-linux-${arch}.json"
}

install_cargo
clean_build_workspace
build_binaries
execute_build
build_snowflake_managed_tarball
