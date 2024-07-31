#!/bin/env bash
set -o pipefail

git config --global --add safe.directory /snowflake-cli

ROOT_DIR="$(git rev-parse --show-toplevel)"
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCH="$(uname -m)"
PKG_NAME="snowflake-cli"
VERSION=$(hatch version)
DEB_PGK_FILE_NAME="snowflake_cli_${VERSION}.${ARCH}.deb"
RPM_PGK_FILE_NAME="snowflake_cli_${VERSION}.${ARCH}.rpm"

echo "-----------------"
echo ${VERSION}
echo ${THIS_DIR}
echo ${ROOT_DIR}
echo ${ARCH}
echo ${PKG_NAME}
echo ${DEB_PGK_FILE_NAME}
echo ${RPM_PGK_FILE_NAME}

echo "-----------------"
cd ${ROOT_DIR}

echo "Building deb for version ${VERSION} on ${ARCH}..."
echo ${THIS_DIR}
echo ${ROOT_DIR}/dist/snow/
pwd

fpm \
  -s dir \
  -t deb \
  -n ${PKG_NAME} \
  -v ${VERSION} \
  -a native \
  -p ${DEB_PGK_FILE_NAME} \
  -C ${ROOT_DIR}/dist/snow/ \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install ${THIS_DIR}/ubuntu/after_install.sh \
  --after-remove ${THIS_DIR}/ubuntu/after_remove.sh \
  --force

echo "-----------------"

echo "Building rpm for version ${VERSION} on ${ARCH}..."
echo ${THIS_DIR}
echo ${ROOT_DIR}/dist/snow/
pwd

fpm \
  -s dir \
  -t rpm \
  -n ${PKG_NAME} \
  -v ${VERSION} \
  -a native \
  -p ${RPM_PGK_FILE_NAME} \
  -C ${ROOT_DIR}/dist/snow/ \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install ${THIS_DIR}/centos/after_install.sh \
  --after-remove ${THIS_DIR}/centos/after_remove.sh \
  --force
