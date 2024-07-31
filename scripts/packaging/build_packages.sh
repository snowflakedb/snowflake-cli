#!/bin/env bash
set -o pipefail

VERSION=$(hatch version)
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEB_PGK="snowflke-cli-${VERSION}.deb"
RPM_PGK="snowflke-cli-${VERSION}.rpm"

fpm \
  -s dir \
  -t deb \
  --name snow \
  --version ${VERSION} \
  -p ${DEB_PGK} \
  -C ./dist/snow \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install $THIS_DIR/ubuntu/after_install.sh \
  --after-remove $THIS_DIR/ubuntu/after_remove.sh \
  --force

fpm \
  -s dir \
  -t rpm \
  --name snow \
  --version ${VERSION} \
  -p ${RPM_PG} \
  -C ./dist/snow \
  --prefix /usr/lib/snowflake/snowflake-cli \
  --after-install $THIS_DIR/ubuntu/after_install.sh \
  --after-remove $THIS_DIR/ubuntu/after_remove.sh \
  --force
