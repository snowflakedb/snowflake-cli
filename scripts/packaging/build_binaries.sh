#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
ROOT_DIR="$(git rev-parse --show-toplevel)"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)
ENTRY_POINT="src/snowflake/cli/_app/__main__.py"

hatch -e packaging run pyinstaller \
  --name=snow \
  --target-architecture=$MACHINE \
  --onedir \
  --clean \
  --noconfirm \
  --contents-directory=snowflake-cli-${VERSION} \
  ${ENTRY_POINT}

execute_build() {
  cd $DIST_DIR/snow && ./snow
  cd -
}

execute_build
