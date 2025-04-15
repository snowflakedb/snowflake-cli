#!/usr/bin/env bash
set -xeuo pipefail

SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')
MACHINE=$(uname -m | tr '[:upper:]' '[:lower:]')
PLATFORM="${SYSTEM}-${MACHINE}"

echo "--- creating virtualenv ---"
python3.11 -m venv venv
. venv/bin/activate
python --version

echo "--- installing dependencies ---"
pip install hatch

# install cargo
if [[ ${MACHINE} == "arm64" ]]; then
  echo "installing cargo on arm64"
  curl https://sh.rustup.rs -sSf | bash -s -- -y
elif [[ ${MACHINE} == "x86_64" ]]; then
  echo "installing cargo on x86_64"
  curl https://sh.rustup.rs -sSf | bash -s -- -y --no-modify-path
  source $HOME/.cargo/env
else
  echo "Unsupported machine: ${MACHINE}"
  exit 1
fi
rustup default stable


echo "--- setup variables ---"
BRANCH=${branch}
REVISION=$(git rev-parse ${svnRevision})
CLI_VERSION=$(hatch version)

STAGE_URL="s3://sfc-eng-jenkins/repository/snowflake-cli/${releaseType}/${SYSTEM}_${MACHINE}/${REVISION}/"

ROOT_DIR=$(git rev-parse --show-toplevel)
PACKAGING_DIR=$ROOT_DIR/scripts/packaging
DIST_DIR=$ROOT_DIR/dist

BINARY_NAME="snow-${CLI_VERSION}"
APP_NAME="SnowflakeCLI.app"
APP_DIR=$DIST_DIR/app
APP_SCRIPTS=$APP_DIR/scripts
CODESIGN_IDENTITY="Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)"
PRODUCTSIGN_IDENTITY="Developer ID Installer: Snowflake Computing INC. (W4NT6CRQ7U)"


loginfo() {
  logger -s -p INFO -- $1
}

clean_build_workspace() {
  rm -rf $DIST_DIR || true
}

create_app_template() {
  rm -r ${APP_DIR}/${APP_NAME} || true
  mkdir -p ${APP_DIR}/${APP_NAME}/Contents/MacOS
  mkdir -p ${APP_DIR}/${APP_NAME}/Contents/Resources
}

security -v unlock-keychain -p $MAC_USERNAME_PASSWORD login.keychain-db

loginfo "---------------------------------"
security find-identity -v -p codesigning
loginfo "---------------------------------"

echo "--- build binary ---"

clean_build_workspace
hatch -e packaging run build-isolated-binary
create_app_template
mv $DIST_DIR/binary/${BINARY_NAME} ${APP_DIR}/${APP_NAME}/Contents/MacOS/snow
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

rm ${ROOT_DIR}/asker.sh || true

cat <<ASKPASS >${ROOT_DIR}/asker.sh
#!/usr/bin/env bash
printf "%s\n" "$MAC_USERNAME_PASSWORD"
ASKPASS

chmod +x ${ROOT_DIR}/asker.sh
export SUDO_ASKPASS=${ROOT_DIR}/asker.sh

validate_installation() {
  local pkg_name=$1
  ls -la $pkg_name

  arch -${MACHINE} sudo -A installer -pkg $pkg_name -target /
  [ -f /Applications/${APP_NAME}/Contents/MacOS/snow ]
  PATH=/Applications/${APP_NAME}/Contents/MacOS:$PATH snow

  sudo rm -rf /Applications/${APP_NAME} || true
}

validate_installation $DIST_DIR/snowflake-cli-${CLI_VERSION}-${SYSTEM}-${MACHINE}.pkg

echo "--- Upload artifacts to AWS ---"
ls -la ./dist/
echo "${STAGE_URL}"
command -v aws
aws s3 cp ./dist/ ${STAGE_URL} --recursive --exclude "*" --include="snowflake-cli-${CLI_VERSION}*.pkg"
