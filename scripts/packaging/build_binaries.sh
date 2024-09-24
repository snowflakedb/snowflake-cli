#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

ROOT_DIR="$(git rev-parse --show-toplevel)"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)
ENTRY_POINT="src/snowflake/cli/_app/__main__.py"

hatch -e packaging run pyinstaller \
  --name=snow \
  --onedir \
  --clean \
  --noconfirm \
  --contents-directory=snowflake-cli-${VERSION} \
  ${ENTRY_POINT}

cd $DIST_DIR/snow
./snow

cd $ROOT_DIR
