#!/bin/env bash
set -o pipefail

VERSION=$(hatch version)

hatch -e packaging run pyinstaller \
  --name=snow \
  --onedir \
  --clean \
  --noconfirm \
  --contents-directory=snowflake-cli-${VERSION} \
  src/snowflake/cli/app/__main__.py
