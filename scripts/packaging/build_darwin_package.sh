#!/usr/bin/env bash
set -xeuo pipefail

git config --global --add safe.directory /snowflake-cli
brew install -q tree

ROOT_DIR=$(git rev-parse --show-toplevel)
PACKAGING_DIR=$ROOT_DIR/scripts/packaging

SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')

CLI_VERSION=$(hatch version)

ENTRY_POINT="src/snowflake/cli/_app/__main__.py"
BUILD_DIR="${ROOT_DIR}/build"
DIST_DIR=$ROOT_DIR/dist
APP_NAME="snow"
APP_DIR=$DIST_DIR/app
APP_SCRIPTS=$DIST_DIR/scripts

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
  --name=${APP_NAME} \
  --target-architecture=$MACHINE \
  --onedir \
  --clean \
  --noconfirm \
  --windowed \
  --osx-bundle-identifier=com.snowflake.snowflake-cli \
  --osx-entitlements-file=scripts/packaging/macos/SnowflakeCLI_entitlements.plist \
  --codesign-identity="Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)" \
  --icon=scripts/packaging/macos/snowflake_darwin.icns
${ENTRY_POINT}

$DIST_DIR/${APP_NAME}.app/Contents/MacOS/snow --help

cat >${DIST_DIR}/${APP_NAME}.app/Contents/Info.plist <<INFO_PLIST
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

cp -r $PACKAGING_DIR/macos/snowflake_darwin.icns $DIST_DIR/${APP_NAME}.app/Contents/Resources/SnowflakeCLI.icns
cp -r $PACKAGING_DIR/macos/SnowflakeCLI.bash $DIST_DIR/${APP_NAME}.app/Contents/MacOS/SnowflakeCLI.bash
chmod +x $DIST_DIR/${APP_NAME}.app/Contents/MacOS/SnowflakeCLI.bash

mkdir $DIST_DIR/app/ || true
mv $DIST_DIR/${APP_NAME}.app $APP_DIR/

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
loginfo "---------------------------------"
loginfo "Package build $DIST_DIR/snowflake-cli-${SYSTEM}.unsigned.pkg "
loginfo "---------------------------------"
pkgbuild \
  --identifier com.snowflake.snowflake-cli \
  --install-location '/Applications' \
  --version $CLI_VERSION \
  --scripts $APP_SCRIPTS \
  --root $APP_DIR \
  --component-plist $PACKAGING_DIR/macos/SnowflakeCLI.plist \
  $DIST_DIR/snowflake-cli-${SYSTEM}.unsigned.pkg

ls -l $DIST_DIR

loginfo "---------------------------------"
loginfo "Procuct sign $DIST_DIR/snowflake-cli-${SYSTEM}.unsigned.pkg -> $DIST_DIR/snowflake-cli-${SYSTEM}.pkg"
loginfo "---------------------------------"
productsign \
  --sign "Developer ID Installer: Snowflake Computing INC. (W4NT6CRQ7U)" \
  $DIST_DIR/snowflake-cli-${SYSTEM}.unsigned.pkg \
  $DIST_DIR/snowflake-cli-${SYSTEM}.pkg

ls -l $DIST_DIR

loginfo "---------------------------------"
loginfo "Procuct build $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.unsigned.pkg <- $DIST_DIR/snowflake-cli-${SYSTEM}.pkg"
loginfo "---------------------------------"
productbuild \
  --distribution $PACKAGING_DIR/macos/Distribution.xml \
  --version $CLI_VERSION \
  --resources $PACKAGING_DIR/macos/Resources \
  --package-path $DIST_DIR \
  $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.unsigned.pkg

ls -l $DIST_DIR

loginfo "---------------------------------"
loginfo "Procuct sign $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.unsigned.pkg -> $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.pkg"
loginfo "---------------------------------"
productsign \
  --sign "Developer ID Installer: Snowflake Computing INC. (W4NT6CRQ7U)" \
  $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.unsigned.pkg \
  $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.pkg

cp -p \
  $DIST_DIR/snowflake-cli-${SYSTEM}-${MACHINE}.pkg \
  $DIST_DIR/snowflake-cli-${CLI_VERSION}-${SYSTEM}-${MACHINE}.pkg

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
  [ -f /Applications/SnowflakeCLI.app/Contents/MacOS/snow/snow ]
  PATH=/Applications/SnowflakeCLI.app/Contents/MacOS/snow:$PATH snow

  sudo rm -rf /Applications/SnowflakeCLI.app || true
}

validate_installation $DIST_DIR/snowflake-cli-${CLI_VERSION}-${SYSTEM}-${MACHINE}.pkg
