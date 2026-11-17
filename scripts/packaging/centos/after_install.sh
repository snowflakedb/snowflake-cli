#!/bin/bash -e
#
# RPM %post and %posttrans. %posttrans runs after the previous version's
# %postun on upgrade, which puts the symlink back when that older script
# deletes /usr/local/bin/snow unconditionally.
#
set -o pipefail

SNOWFLAKE_CLI_LINK=/usr/local/bin/snow
SNOWFLAKE_CLI_BIN=/usr/lib/snowflake/snowflake-cli/snow
rm -f ${SNOWFLAKE_CLI_LINK}
ln -s ${SNOWFLAKE_CLI_BIN} ${SNOWFLAKE_CLI_LINK}
