#!/usr/bin/env bash
set -xeuo pipefail

git config --global --add safe.directory /snowflake-cli
brew install -q tree

ROOT_DIR=$(git rev-parse --show-toplevel)
PACKAGING_DIR=$ROOT_DIR/scripts/packaging

SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')

CLI_VERSION=$(hatch version)

DIST_DIR=$ROOT_DIR/dist
APP_NAME=SnowflakeCLI.app
APP_DIR=$DIST_DIR/app
APP_SCRIPTS=$DIST_DIR/scripts

loginfo() {
  logger -s -p INFO -- $1
}

$DIST_DIR/snow/snow --help

loginfo "Building darwin package for version ${CLI_VERSION}"

setup_app_dir() {
  rm -rf $APP_DIR || true
  mkdir -p $APP_DIR/$APP_NAME/Contents/{MacOS,Resources} || true
  tree $APP_DIR
}

setup_app_dir
cd $APP_DIR

cat >$APP_NAME/Contents/Info.plist <<INFO_PLIST
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

cp -r $DIST_DIR/snow $APP_NAME/Contents/MacOS/
cp -r $PACKAGING_DIR/macos/snowflake_darwin.icns $APP_NAME/Contents/Resources/SnowflakeCLI.icns
cp -r $PACKAGING_DIR/macos/SnowflakeCLI.bash $APP_NAME/Contents/MacOS/SnowflakeCLI.bash
chmod +x $APP_NAME/Contents/MacOS/SnowflakeCLI.bash

tree -d $DIST_DIR

security -v unlock-keychain -p $MAC_USERNAME_PASSWORD login.keychain-db

loginfo "---------------------------------"
security find-identity -v -p codesigning
loginfo "---------------------------------"

code_sign() {
  ENTITLEMENTS=$PACKAGING_DIR/macos/SnowflakeCLI_entitlements.plist
  loginfo "---------------------------------"
  loginfo "Code signing $1"
  loginfo "---------------------------------"
  ls -l $1
  codesign \
    --timestamp \
    --deep \
    --force \
    --entitlements $ENTITLEMENTS \
    --options=runtime \
    --sign "Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)" \
    $1
}

code_sign_no_runtime() {
  ENTITLEMENTS=$PACKAGING_DIR/macos/SnowflakeCLI_entitlements.plist
  loginfo "---------------------------------"
  loginfo "Code signing $1 no runtime"
  loginfo "---------------------------------"
  codesign \
    --timestamp \
    --deep \
    --force \
    --entitlements $ENTITLEMENTS \
    --sign "Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)" \
    $1
}

code_sign_validate() {
  loginfo "---------------------------------"
  loginfo "Validating code signing for $1"
  loginfo "---------------------------------"
  codesign -dvvv --force $1
}

APP_CONTENTS=$APP_NAME/Contents/MacOS/snow
ENTITLEMENTS=$PACKAGING_DIR/macos/SnowflakeCLI_entitlements.plist

sign_runtime_binary() {
  code_sign $APP_CONTENTS/snow
  code_sign_validate $APP_CONTENTS/snow
}

loginfo "-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-"
find $APP_CONTENTS/snowflake-cli-* -type f -name 'Python' -ls
for p in $(find $APP_CONTENTS/snowflake-cli-* -type f -name 'Python'); do
  code_sign $p
  code_sign_validate $p
done
loginfo "-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-"

sign_no_runtime_binary() {
  # all executables and shared libraries
  for item in $(find $APP_CONTENTS/snowflake-cli-* -type f -name '*.so' -or -name '*.dylib'); do
    code_sign_no_runtime $item
    code_sign_validate $item
  done
}

sign_runtime_binary
sign_no_runtime_binary

# POSTINSTALL SCRIPT
prepare_postinstall_script() {
  rm -rf $APP_SCRIPTS || true
  mkdir -p $APP_SCRIPTS || true
  cp -r $PACKAGING_DIR/macos/postinstall $APP_SCRIPTS/postinstall
}

prepare_postinstall_script

ls -l $DIST_DIR

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
