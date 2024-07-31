#!/bin/env bash
set -o pipefail

VERSION=$(hatch version)
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCH="$(uname -m)"
DEB_PGK="snowflake_cli_${VERSION}.${ARCH}.deb"
RPM_PGK="snowflake_cli_${VERSION}.${ARCH}.rpm"

echo "Building deb for version ${VERSION} on ${ARCH}..."
fpm \
  -s dir \
  -t deb \
  --name snow \
  --version ${VERSION} \
  -a native \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install $THIS_DIR/ubuntu/after_install.sh \
  --after-remove $THIS_DIR/ubuntu/after_remove.sh \
  --force \
  -C ./dist/snow \
  -p ${DEB_PGK}

echo "Building rpm for version ${VERSION} on ${ARCH}..."
fpm \
  -s dir \
  -t rpm \
  --name snow \
  --version ${VERSION} \
  -a native \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install $THIS_DIR/centos/after_install.sh \
  --after-remove $THIS_DIR/centos/after_remove.sh \
  --force \
  -C ./dist/snow \
  -p ${RPM_PKG}
