#!/bin/bash
set -eo pipefail

git config --global --add safe.directory /snowflake-cli

cd $(git rev-parse --show-toplevel)

python .compat/snowflake-cli-labs_version_sync.py

cd .compat/snowflake-cli-labs
# Nested labs has no [tool.hatch.envs.packaging]. hatch run packaging:legacy-pypi-build
# exports HATCH_ENV_ACTIVE=packaging; a bare hatch build then aborts
# (Unknown environment: packaging).
env -u HATCH_ENV -u HATCH_ENV_ACTIVE hatch build --clean
