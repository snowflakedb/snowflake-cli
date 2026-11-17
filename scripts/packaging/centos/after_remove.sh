#!/bin/bash -e
#
# RPM %postun. $1 is 0 on erase and >= 1 on upgrade. Only remove the
# symlink on erase. On upgrade, %posttrans recreates it after the
# previous release's %postun, which still deletes the link unconditionally.
#
set -o pipefail

SNOWFLAKE_CLI_LINK=/usr/local/bin/snow
if [[ "${1:-}" == "0" && -L "${SNOWFLAKE_CLI_LINK}" ]]; then
  # delete only if it is symlink
  rm -f "${SNOWFLAKE_CLI_LINK}"
fi
