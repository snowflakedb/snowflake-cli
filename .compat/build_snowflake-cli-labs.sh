#!/bin/sh
set -eo pipefail

cd $(git rev-parse --show-toplevel)

python .compat/snowflake-cli-labs_version_sync.py

cd .compat/snowflake-cli-labs
hatch build --clean
