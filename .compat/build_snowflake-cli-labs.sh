#!/bin/bash
set -eo pipefail

git config --global --add safe.directory /snowflake-cli

cd $(git rev-parse --show-toplevel)

python .compat/snowflake-cli-labs_version_sync.py

cd .compat/snowflake-cli-labs
hatch build --clean
