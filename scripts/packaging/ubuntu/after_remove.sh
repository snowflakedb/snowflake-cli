#!/bin/bash -e
#
# Snowcd after remove script
#
set -o pipefail

SNOWFLAKE_CLI_LINK=/usr/local/bin/snow
if [[ -L "${SNOWFLAKE_CLI_LINK}" ]]; then
  # delete only if it is symlink
  rm -f "${SNOWFLAKE_CLI_LINK}"
fi
