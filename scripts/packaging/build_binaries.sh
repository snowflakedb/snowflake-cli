#!/usr/bin/env bash
set -o pipefail

VERSION=$(hatch version)
ENTRY_POINT="src/snowflake/cli/_app/__main__.py"

hatch -e packaging run pyinstaller \
  --name=snow \
  --onedir \
  --clean \
  --noconfirm \
  --contents-directory=snowflake-cli-${VERSION} \
  ${ENTRY_POINT}
