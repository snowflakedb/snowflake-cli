#!/usr/bin/env bash
set -oxeu pipefail

git config --global --add safe.directory /snowflake-cli

ROOT_DIR="$(git rev-parse --show-toplevel)"
THIS_DIR=$(dirname $(readlink -f $0))
DIST_DIR="${ROOT_DIR}/dist"
ARCH="$(uname -m)"
PKG_NAME="snowflake-cli"
VERSION=$(hatch version)
DEB_PGK_FILE_NAME="snowflake-cli-${VERSION}.${ARCH}.deb"
RPM_PGK_FILE_NAME="snowflake-cli-${VERSION}.${ARCH}.rpm"

echo "-*-*-*- build variables -*-*-*-"
echo ${VERSION}
echo ${THIS_DIR}
echo ${ROOT_DIR}
echo ${DIST_DIR}
echo ${ARCH}
echo ${PKG_NAME}
echo ${DEB_PGK_FILE_NAME}
echo ${RPM_PGK_FILE_NAME}
echo "-*-*-*- build variables -*-*-*-"

cd $DIST_DIR/snow
./snow

cd ${ROOT_DIR}

echo "-*-*-*- Building deb for version ${VERSION} on ${ARCH}... -*-*-*-"

fpm \
  -s dir \
  -t deb \
  -n ${PKG_NAME} \
  -v ${VERSION} \
  -a native \
  -p ${ROOT_DIR}/dist/${DEB_PGK_FILE_NAME} \
  -C ${ROOT_DIR}/dist/snow/ \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install ${THIS_DIR}/ubuntu/after_install.sh \
  --after-remove ${THIS_DIR}/ubuntu/after_remove.sh \
  --force

echo "-----------------"

echo "-*-*-*- Building rpm for version ${VERSION} on ${ARCH}... -*-*-*-"
pwd

fpm \
  -s dir \
  -t rpm \
  -n ${PKG_NAME} \
  -v ${VERSION} \
  -a native \
  -p ${ROOT_DIR}/dist/${RPM_PGK_FILE_NAME} \
  -C ${ROOT_DIR}/dist/snow/ \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install ${THIS_DIR}/centos/after_install.sh \
  --after-remove ${THIS_DIR}/centos/after_remove.sh \
  --force
