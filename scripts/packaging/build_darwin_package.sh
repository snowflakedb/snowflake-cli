#!/usr/bin/env bash
set -o pipefail

git config --global --add safe.directory /snowflake-cli

ROOT_DIR=$(git rev-parse --show-toplevel)
PACKAGING_DIR=$ROOT_DIR/scripts/packaging

CLI_VERSION=$(hatch version)

DIST_DIR=$ROOT_DIR/dist
APP_DIR=$DIST_DIR/app

log INFO "Building darwin package for version $CLI_VERSION"

rm -rf $APP_DIR
mkdir -p $APP_DIR
cd $APP_DIR

mkdir -p SnowflakeCLI.app/Contents/{MacOS,Resources}

cat >SnowflakeCLI.app/Contents/Info.plist <<INFO_PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>SnowflakeCLI</string>
    <key>CFBundleDisplayName</key>
    <string>SnowflakeCLI</string>
    <key>CFBundleIdentifier</key>
    <string>net.snowflake.snowsql</string>
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

cp -r $DIST_DIR/snow SnowflakeCLI.app/Contents/MacOS/snow
cp -r $PACKAGING_DIR/macos/snowflake_darwin.icns SnowflakeCLI.app/Contents/Resources/SnowflakeCLI.icns
cp -r $PACKAGING_DIR/macos/Snow.bash SnowflakeCLI.app/Contents/MacOS/SnowflakeCLI.bash

chmod +x SnowflakeCLI.app/Contents/MacOS/SnowflakeCLI.bash

ENTITLEMENTS=$PACKAGING_DIR/macos/SnowflakeCLI_entitlements.plist
alias CSIGN='codesign --force --deep --timestamp --entitlements $ENTITLEMENTS --options=runtime --sign "Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)"'
alias CVAL='codesign -dvvv --force'

CSIGN SnowflakeCLI.app/Contents/MacOS/snow
CVAL SnowflakeCLI.app/Contents/MacOS/snow

CSIGN SnowflakeCLI.app
CVAL SnowflakeCLI.app
