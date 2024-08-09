#!/bin/bash -e
#
# Snowcd after install script
set -o pipefail

SNOWFLAKE_CLI_LINK=/usr/local/bin/snow
SNOWFLAKE_CLI_BIN=/usr/lib/snowflake/snowflake-cli/snow
rm -f ${SNOWFLAKE_CLI_LINK}
ln -s ${SNOWFLAKE_CLI_BIN} ${SNOWFLAKE_CLI_LINK}
