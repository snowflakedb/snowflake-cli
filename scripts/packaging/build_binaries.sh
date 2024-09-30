#!/usr/bin/env bash
set -oeux pipefail

git config --global --add safe.directory /snowflake-cli

MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
ROOT_DIR="$(git rev-parse --show-toplevel)"
DIST_DIR="${ROOT_DIR}/dist"

VERSION=$(hatch version)
ENTRY_POINT="src/snowflake/cli/_app/__main__.py"

if [[ ${SYSTEM} == "darwin" ]]; then
  hatch -e packaging run pyinstaller \
    --name=snow \
    --target-architecture=$MACHINE \
    --onedir \
    --clean \
    --noconfirm \
    --osx-bundle-identifier=com.snowflake.snowflake-cli \
    --codesign-identity="Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)" \
    --osx-entitlements-file=scripts/packaging/macos/SnowflakeCLI_entitlements.plist \
    --contents-directory=snowflake-cli-${VERSION} \
    ${ENTRY_POINT}
elif [[ ${SYSTEM} == "linux" ]]; then
  hatch -e packaging run pyinstaller \
    --name=snow \
    --target-architecture=$MACHINE \
    --onedir \
    --clean \
    --noconfirm \
    --contents-directory=snowflake-cli-${VERSION} \
    ${ENTRY_POINT}
fi

execute_build() {
  cd $DIST_DIR/snow && ./snow
  cd -
}

execute_build
