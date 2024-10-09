#!/usr/bin/env bash
set -xeuo pipefail

git config --global --add safe.directory /snowflake-cli
brew install -q tree

ROOT_DIR=$(git rev-parse --show-toplevel)
PACKAGING_DIR=$ROOT_DIR/scripts/packaging

SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
PLATFORM="${SYSTEM}-${MACHINE}"

CLI_VERSION=$(hatch version)

ENTRY_POINT="src/snowflake/cli/_app/__main__.py"
BUILD_DIR="${ROOT_DIR}/build"
DIST_DIR=$ROOT_DIR/dist
BINARY_NAME="snow"
APP_NAME="SnowflakeCLI.app"
APP_DIR=$DIST_DIR/app
APP_SCRIPTS=$APP_DIR/scripts
CODESIGN_IDENTITY="Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)"
PRODUCTSIGN_IDENTITY="Developer ID Installer: Snowflake Computing INC. (W4NT6CRQ7U)"

loginfo() {
  logger -s -p INFO -- $1
}

clean_build_workspace() {
  rm -rf $DIST_DIR $BUILD_DIR || true
}

clean_build_workspace

security -v unlock-keychain -p $MAC_USERNAME_PASSWORD login.keychain-db

loginfo "---------------------------------"
security find-identity -v -p codesigning
loginfo "---------------------------------"

hatch -e packaging run pyinstaller \
  --name=${BINARY_NAME} \
  --target-architecture=$MACHINE \
  --onedir \
  --clean \
  --noconfirm \
  --windowed \
  --osx-bundle-identifier=com.snowflake.snowflake-cli \
  --osx-entitlements-file=scripts/packaging/macos/SnowflakeCLI_entitlements.plist \
  --codesign-identity="${CODESIGN_IDENTITY}" \
  --icon=scripts/packaging/macos/snowflake_darwin.icns \
  ${ENTRY_POINT}

ls -l $DIST_DIR
mkdir $APP_DIR || true
mv $DIST_DIR/${BINARY_NAME}.app ${APP_DIR}/${APP_NAME}
${APP_DIR}/${APP_NAME}/Contents/MacOS/snow --help

cat >${APP_DIR}/${APP_NAME}/Contents/Info.plist <<INFO_PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>SnowflakeCLI</string>
    <key>CFBundleDisplayName</key>
    <string>SnowflakeCLI</string>
    <key>CFBundleIdentifier</key>
    <string>com.snowflake.snowflake-cli</string>
    <key>CFBundleVersion</key>
    <string>$CLI_VERSION</string>
    <key>CFBundleShortVersionString</key>
    <string>$CLI_VERSION</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleExecutable</key>
    <string>SnowflakeCLI.bash</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleSignature</key>
    <string>snow</string>
    <key>NSHumanReadableCopyright</key>
    <string>Copyright © 2016-2019 Snowflake, Inc. All rights reserved.</string>
    <key>CFBundleGetInfoString</key>
    <string>$CLI_VERSION Copyright © 2016-2019 Snowflake, Inc. All rights reserved.</string>
    <key>CFBundleIconFile</key>
    <string>SnowflakeCLI.icns</string>
</dict>
</plist>
INFO_PLIST

cp -r $PACKAGING_DIR/macos/snowflake_darwin.icns ${APP_DIR}/${APP_NAME}/Contents/Resources/SnowflakeCLI.icns
cp -r $PACKAGING_DIR/macos/SnowflakeCLI.bash ${APP_DIR}/${APP_NAME}/Contents/MacOS/SnowflakeCLI.bash
chmod +x $APP_DIR/${APP_NAME}/Contents/MacOS/SnowflakeCLI.bash

# POSTINSTALL SCRIPT
prepare_postinstall_script() {
  rm -rf $APP_SCRIPTS || true
  mkdir -p $APP_SCRIPTS || true
  cp -r $PACKAGING_DIR/macos/postinstall $APP_SCRIPTS/postinstall
}

prepare_postinstall_script

ls -l $DIST_DIR
tree -d $DIST_DIR

chmod +x $APP_SCRIPTS/postinstall

# codesign after changes
codesign \
  --timestamp \
  --deep \
  --force \
  --verify \
  --verbose \
  --options runtime \
  --entitlements $PACKAGING_DIR/macos/SnowflakeCLI_entitlements.plist \
  --sign "${CODESIGN_IDENTITY}" ${APP_DIR}/${APP_NAME}

PKG_UNSIGNED_NAME="snowflake-cli-${SYSTEM}.unsigned.pkg"
loginfo "---------------------------------"
loginfo "Package build ${DIST_DIR}/${PKG_UNSIGNED_NAME}"
loginfo "---------------------------------"
pkgbuild \
  --identifier com.snowflake.snowflake-cli \
  --install-location '/Applications' \
  --version $CLI_VERSION \
  --scripts $APP_SCRIPTS \
  --root $APP_DIR \
  --component-plist ${PACKAGING_DIR}/macos/SnowflakeCLI.plist \
  ${DIST_DIR}/${PKG_UNSIGNED_NAME}

ls -l $DIST_DIR

PRODUCT_UNSIGNED_NAME="snowflake-cli-${SYSTEM}.unsigned.pkg"
PRODUCT_SIGNED_NAME="snowflake-cli-${SYSTEM}.pkg"
loginfo "---------------------------------"
loginfo "Procuct sign ${DIST_DIR}/${PRODUCT_UNSIGNED_NAME} -> ${DIST_DIR}/${PRODUCT_SIGNED_NAME}"
loginfo "---------------------------------"
productsign \
  --sign "${PRODUCTSIGN_IDENTITY}" \
  ${DIST_DIR}/${PRODUCT_UNSIGNED_NAME} \
  ${DIST_DIR}/${PRODUCT_SIGNED_NAME}

ls -l $DIST_DIR

PRODUCT_BUILD_UNSIGNED_NAME="snowflake-cli-${PLATFORM}.unsigned.pkg"
loginfo "---------------------------------"
loginfo "Procuct build ${DIST_DIR}/${PRODUCT_BUILD_UNSIGNED_NAME} <- ${DIST_DIR}/${PRODUCT_SIGNED_NAME}"
loginfo "---------------------------------"
productbuild \
  --distribution $PACKAGING_DIR/macos/Distribution.xml \
  --version $CLI_VERSION \
  --resources $PACKAGING_DIR/macos/Resources \
  --package-path $DIST_DIR \
  ${DIST_DIR}/${PRODUCT_BUILD_UNSIGNED_NAME}

ls -l $DIST_DIR

PRODUCT_BUILD_SIGNED_NAME="snowflake-cli-${PLATFORM}.pkg"
loginfo "---------------------------------"
loginfo "Procuct sign ${DIST_DIR}${PRODUCT_BUILD_UNSIGNED_NAME} -> ${DIST_DIR}/${PRODUCT_BUILD_SIGNED_NAME}"
loginfo "---------------------------------"
productsign \
  --sign "${PRODUCTSIGN_IDENTITY}" \
  ${DIST_DIR}/${PRODUCT_BUILD_UNSIGNED_NAME} \
  ${DIST_DIR}/${PRODUCT_BUILD_SIGNED_NAME}

FINAL_PKG_NAME="snowflake-cli-${CLI_VERSION}-${PLATFORM}.pkg"
cp -p ${DIST_DIR}/${PRODUCT_BUILD_SIGNED_NAME} ${DIST_DIR}/${FINAL_PKG_NAME}

ls -l $DIST_DIR

cat <<ASKPASS >./asker.sh
  #!/bin/bash
  printf "%s\n" "$MAC_USERNAME_PASSWORD"
ASKPASS

validate_installation() {
  local pkg_name=$1
  ls -la $pkg_name

  export SUDO_ASKPASS=./asker.sh
  sudo -A installer -pkg $pkg_name -target /
  [ -f /Applications/${APP_NAME}/Contents/MacOS/snow ]
  PATH=/Applications/${APP_NAME}/Contents/MacOS:$PATH snow

  sudo rm -rf /Applications/${APP_NAME} || true
}

validate_installation $DIST_DIR/snowflake-cli-${CLI_VERSION}-${SYSTEM}-${MACHINE}.pkg
